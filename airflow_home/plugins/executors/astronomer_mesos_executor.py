import os
import logging
from queue import Queue
from builtins import str
from future import standard_library

from pymesos import MesosSchedulerDriver, Scheduler
from addict import Dict

from airflow import configuration
from airflow.models import DagPickle
from airflow.executors.base_executor import BaseExecutor
from airflow.settings import Session
from airflow.utils.state import State
from airflow.exceptions import AirflowException
from airflow.utils.operator_resources import (
    ScalarResource, TextResource
)

standard_library.install_aliases()

DEFAULT_FRAMEWORK_NAME = 'Airflow'
FRAMEWORK_CONNID_PREFIX = 'mesos_framework_'


def get_framework_name():
    if not configuration.get('mesos', 'FRAMEWORK_NAME'):
        return DEFAULT_FRAMEWORK_NAME
    return configuration.get('mesos', 'FRAMEWORK_NAME')


def copy_env_var(command, env_var_name):
    if not isinstance(command.environment.variables, list):
        command.environment.variables = []
    command.environment.variables.append(
        dict(name=env_var_name, value=os.getenv(env_var_name, ''))
    )


def offer_suitable(mesos_offer, airflow_task):
    airflow_cpu = airflow_task.resources.cpu
    mesos_cpu = [x for x in mesos_offer.resources if x.name == 'cpus']
    if len(mesos_cpu) > 0:
        totalOffer = sum(x.scalar.value for x in mesos_cpu)
        if totalOffer < airflow_cpu.value:
            print("not enough cpu in offer")
            return False
    else:
        print("no enough cpu in offer")
        return False  # not enough cpu

    airflow_ram = airflow_task.resources.ram
    mesos_mem = [x for x in mesos_offer.resources if x.name == 'mem']
    if len(mesos_mem) > 0:
        totalOffer = sum(x.scalar.value for x in mesos_mem)
        if totalOffer < airflow_ram.value:
            print("not enough mem in offer")
            return False
    else:
        print("no mem in offer")
        return False

    #TODO: check for disk
    mesos_org = next((x for x in mesos_offer.attributes if x.name == 'organizationId'), Dict())
    if airflow_task.resources.organizationId is not None:
        offer_organization_matches = airflow_task.resources.organizationId.value == mesos_org.text.value
        if offer_organization_matches:
            print("offer_organization_matches true")
        else:
            print("offer_organization_matches false")
        return offer_organization_matches
    elif len(mesos_org.text.value) > 0:
        print("no org attribute in offer")
        return False
    print("offer fits task")
    return True


# AirflowMesosScheduler, implements Mesos Scheduler interface
# To schedule airflow jobs on mesos
class AirflowMesosScheduler(Scheduler):
    """
    Airflow Mesos scheduler implements mesos scheduler interface
    to schedule airflow tasks on mesos.
    Basically, it schedules a command like
    'airflow run <dag_id> <task_instance_id> <start_date> --local -p=<pickle>'
    to run on a mesos slave.
    """

    def __init__(self,
                 task_queue,
                 result_queue):
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task_counter = 0
        self.task_key_map = {}

    def registered(self, driver, frameworkId, masterInfo):
        logging.info("AirflowScheduler registered to mesos with framework ID %s", frameworkId.value)

        if configuration.getboolean('mesos', 'CHECKPOINT') and configuration.get('mesos', 'FAILOVER_TIMEOUT'):
            # Import here to work around a circular import error
            from airflow.models import Connection

            # Update the Framework ID in the database.
            session = Session()
            conn_id = FRAMEWORK_CONNID_PREFIX + get_framework_name()
            connection = Session.query(Connection).filter_by(conn_id=conn_id).first()
            if connection is None:
                connection = Connection(conn_id=conn_id, conn_type='mesos_framework-id',
                                        extra=frameworkId.value)
            else:
                connection.extra = frameworkId.value

            session.add(connection)
            session.commit()
            Session.remove()

    def reregistered(self, driver, masterInfo):
        logging.info("AirflowScheduler re-registered to mesos")

    def disconnected(self, driver):
        logging.info("AirflowScheduler disconnected from mesos")

    def offerRescinded(self, driver, offerId):
        logging.info("AirflowScheduler offer %s rescinded", str(offerId))

    def frameworkMessage(self, driver, executorId, slaveId, message):
        logging.info("AirflowScheduler received framework message %s", message)

    def executorLost(self, driver, executorId, slaveId, status):
        logging.warning("AirflowScheduler executor %s lost", str(executorId))

    def slaveLost(self, driver, slaveId):
        logging.warning("AirflowScheduler slave %s lost", str(slaveId))

    def error(self, driver, message):
        logging.error("AirflowScheduler driver aborted %s", message)
        raise AirflowException("AirflowScheduler driver aborted %s" % message)

    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []

            while (not self.task_queue.empty()):
                key, cmd, task_instance = self.task_queue.get()
                # validate resource offers
                if not offer_suitable(offer, task_instance):
                    # if not suitable, put task back on the queue
                    print("offer not suitable for {}".format(key))
                    print("offer={}".format(offer))
                    print("task_instance.resources={}".format(task_instance.resources))
                    self.task_queue.put((key, cmd, task_instance))
                    break
                tid = self.task_counter
                self.task_counter += 1
                self.task_key_map[str(tid)] = key

                logging.info("Launching task %d using offer %s", tid, offer.id.value)

                task = Dict()
                task.task_id.value = str(tid)
                task.agent_id.value = offer.agent_id.value
                task.name = "AirflowTask %d" % tid
                task.resources = [
                    dict(name="cpus", type="SCALAR", scalar={"value": task_instance.resources.cpu.value}),
                    dict(name="mem", type="SCALAR", scalar={"value": task_instance.resources.ram.value}),
                ]

                container = Dict()
                container.type = "DOCKER"
                container.volumes = [
                    dict(host_path="/airflow_home/logs", container_path="/airflow_home/logs", mode="RW"),
                    dict(host_path="/var/run/docker.sock", container_path="/var/run/docker.sock", mode="RW"),
                ]

                docker = Dict()
                docker.image = os.getenv("DOCKER_AIRFLOW_IMAGE_TAG", "astronomerio/airflow")
                docker.force_pull_image = True

                container.docker = docker
                task.container = container

                command = Dict()
                command.value = cmd

                # Copy some environment vars from scheduler to execution docker container
                copy_env_var(command, "AIRFLOW__CORE__SQL_ALCHEMY_CONN")
                copy_env_var(command, "AWS_ACCESS_KEY_ID")
                copy_env_var(command, "AWS_SECRET_ACCESS_KEY")

                task.command = command
                logging.info("Launching task: %s", task)
                tasks.append(task)

            driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, update):
        logging.info("Task %s is in state %s, data %s",
                     update.task_id.value, update.state, update)

        try:
            key = self.task_key_map[update.task_id.value]
        except KeyError:
            # The map may not contain an item if the framework re-registered after a failover.
            # Discard these tasks.
            logging.warn("Unrecognised task key %s" % update.task_id.value)
            return

        # XXX: Sometimes we get into a situation where task_queue.task_done()
        # throws errors. Could be due to some unhandled event we should be taking
        # care of somewhere else. Less likely, could be due to an issue where Queue.put isn't
        # properly locking.  Either way, just ignore for now.
        try:
            if update.state == "TASK_FINISHED":
                self.result_queue.put((key, State.SUCCESS))
                self.task_queue.task_done()

            if update.state == "TASK_LOST" or \
               update.state == "TASK_KILLED" or \
               update.state == "TASK_FAILED":
                self.result_queue.put((key, State.FAILED))
                self.task_queue.task_done()
        except ValueError:
            logging.warn("Error marking task_done")


class AstronomerMesosExecutor(BaseExecutor):
    """
    MesosExecutor allows distributing the execution of task
    instances to multiple mesos workers.

    Apache Mesos is a distributed systems kernel which abstracts
    CPU, memory, storage, and other compute resources away from
    machines (physical or virtual), enabling fault-tolerant and
    elastic distributed systems to easily be built and run effectively.
    See http://mesos.apache.org/
    """

    def __init__(self, mesos_driver=None):
        super().__init__()
        self.task_queue = Queue()
        self.result_queue = Queue()
        self._mesos_driver = mesos_driver

    @property
    def mesos_driver(self):
        """
        Lazily instantiates the Mesos scheduler driver if one was not injected in
        via the constructor
        """
        if self._mesos_driver is None:
            framework = Dict()
            framework.user = 'core'

            if not configuration.get('mesos', 'MASTER'):
                logging.error("Expecting mesos master URL for mesos executor")
                raise AirflowException("mesos.master not provided for mesos executor")

            master = configuration.get('mesos', 'MASTER')

            framework.name = get_framework_name()

            if configuration.getboolean('mesos', 'CHECKPOINT'):
                framework.checkpoint = True

                if configuration.get('mesos', 'FAILOVER_TIMEOUT'):
                    # Import here to work around a circular import error
                    from airflow.models import Connection

                    # Query the database to get the ID of the Mesos Framework, if available.
                    conn_id = FRAMEWORK_CONNID_PREFIX + framework.name
                    session = Session()
                    connection = session.query(Connection).filter_by(conn_id=conn_id).first()
                    if connection is not None:
                        # Set the Framework ID to let the scheduler reconnect with running tasks.
                        framework.id.value = connection.extra

                    framework.failover_timeout = configuration.getint('mesos', 'FAILOVER_TIMEOUT')
            else:
                framework.checkpoint = False

            logging.info('MesosFramework master : %s, name : %s, checkpoint : %s',
                         master, framework.name, str(framework.checkpoint))

            if configuration.getboolean('mesos', 'AUTHENTICATE'):
                if not configuration.get('mesos', 'DEFAULT_PRINCIPAL'):
                    logging.error("Expecting authentication principal in the environment")
                    raise AirflowException("mesos.default_principal not provided in authenticated mode")
                if not configuration.get('mesos', 'DEFAULT_SECRET'):
                    logging.error("Expecting authentication secret in the environment")
                    raise AirflowException("mesos.default_secret not provided in authenticated mode")

                principal = configuration.get('mesos', 'DEFAULT_PRINCIPAL')
                secret = configuration.get('mesos', 'DEFAULT_SECRET')

                framework.principal = credential.principal

                self._mesos_driver = MesosSchedulerDriver(
                    AirflowMesosScheduler(self.task_queue, self.result_queue),
                    framework,
                    master,
                    use_addict=True,
                    principal=principal,
                    secret=secret)
            else:
                framework.principal = 'Airflow'
                self._mesos_driver = MesosSchedulerDriver(
                    AirflowMesosScheduler(self.task_queue, self.result_queue),
                    framework,
                    master,
                    use_addict=True)
        return self._mesos_driver

    def start(self):
        self.mesos_driver.start()

    def execute_async(self, key, command, queue=None):
        logging.info('placing task on queue: %s %s', key, command)
        pickle_id = self.pickle_from_command(command)
        if pickle_id is not None:
            dag_pickle = self.find_pickle(pickle_id)
            if dag_pickle is not None:
                dag = dag_pickle.pickle
                dag_id, task_id, execution_date = key
                task_instance = dag.get_task(task_id=task_id)
                logging.info('Have matching task %s', task_instance)
                logging.info('requires resources: %s', task_instance.resources)
        # query dag_pickle table with pickle from command
        # get the task matching key off dag.tasks
        self.task_queue.put((key, command, task_instance))

    def sync(self):
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        self.task_queue.join()
        self.mesos_driver.stop()

    def pickle_from_command(self, command):

        from airflow.bin.cli import get_parser
        parser = get_parser()
        strip_airflow = command[len('airflow '):]
        args = parser.parse_args(strip_airflow.split())
        if hasattr(args, 'pickle'):
            return args.pickle

    def find_pickle(self, pickle_id):
        session = Session()
        logging.info(f'Loading pickle id {pickle_id}')
        dag_pickle = session.query(
            DagPickle).filter(DagPickle.id == pickle_id).first()
        return dag_pickle
