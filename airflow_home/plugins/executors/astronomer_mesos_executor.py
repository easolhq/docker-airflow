"""
This scheduler and executor combine our environment specific features, bug
fixes, and usage of pymesos to be more robust than the standard MesosExecutor.
"""

import logging
import os
from builtins import str
from future import standard_library
from queue import Queue

from addict import Dict
from pymesos import MesosSchedulerDriver, Scheduler

from airflow import configuration
# from airflow.models import DagPickle
from airflow.executors.base_executor import BaseExecutor
from airflow.settings import Session
from airflow.utils.state import State
from airflow.exceptions import AirflowException
# from airflow.utils.operator_resources import ScalarResource, TextResource

# TODO: get rid of this bad camelCase variable convention


standard_library.install_aliases()

DEFAULT_FRAMEWORK_NAME = 'Airflow'
FRAMEWORK_CONNID_PREFIX = 'mesos_framework_'
# DEFAULT_FRAMEWORK_ROLE = "*"


def get_framework_name():
    if not configuration.get('mesos', 'FRAMEWORK_NAME'):
        return DEFAULT_FRAMEWORK_NAME
    return configuration.get('mesos', 'FRAMEWORK_NAME')


# def get_role():
#     if not configuration.get('mesos', 'FRAMEWORK_ROLE'):
#         # return DEFAULT_ROLE_NAME
#         return DEFAULT_FRAMEWORK_ROLE
#     return configuration.get('mesos', 'FRAMEWORK_ROLE')


def get_task_prefix():
    if not configuration.get('mesos', 'TASK_PREFIX'):
        return DEFAULT_TASK_PREFIX
    return configuration.get('mesos', 'TASK_PREFIX')


def copy_env_var(command, env_var_name):
    if not isinstance(command.environment.variables, list):
        command.environment.variables = []

    # don't propagate environment variables undefined in parent environment
    env_var_value = os.getenv(env_var_name, None)
    if env_var_value is not None:
        env_var = {
            'name': env_var_name,
            'value': env_var_value,
        }
        command.environment.variables.append(env_var)
    else:
        logging.warn(f'environment variable "{env_var_name}" is not defined')


# def offer_suitable(task_instance, cpus=0, mem=0, offerOrgIds=[]):
# def offer_suitable(cpus=0, mem=0):
#     is_suitable = True
#     # if task_instance.resources.cpu.value > cpus:
#     #     logging.debug("offer doesn't have enough cpu")
#     #     is_suitable = False
#     # if task_instance.resources.ram.value > mem:
#     #     logging.debug("offer doesn't have enough mem")
#     #     is_suitable = False
#     # if not hasattr(task_instance.resources, 'organizationId'):
#     #     logging.info("task_instance doesn't have organizationId")
#     #     return False
#     # if task_instance.resources.organizationId.value not in offerOrgIds:
#     #     logging.debug("offer doesn't have organizationId")
#     #     is_suitable = False
#     return is_suitable


class AirflowMesosScheduler(Scheduler):

    def __init__(self, task_queue, result_queue, task_cpu=1, task_mem=256):
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task_cpu = task_cpu
        self.task_mem = task_mem
        self.task_counter = 0
        self.task_key_map = {}

    def registered(self, driver, frameworkId, masterInfo):
        logging.info(f'AirflowScheduler registered to mesos with framework ID {frameworkId.value}')

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
        logging.info('AirflowScheduler re-registered to mesos')

    def disconnected(self, driver):
        # TODO: exponential backoff?
        # TODO: use many scheduler connections as a connection pool
        self.start()
        logging.info('AirflowScheduler disconnected from mesos')

    def offerRescinded(self, driver, offerId):
        logging.info(f'AirflowScheduler offer {offerId} rescinded')

    def frameworkMessage(self, driver, executorId, slaveId, message):
        logging.info(f'AirflowScheduler received framework message {message}')

    def executorLost(self, driver, executorId, slaveId, status):
        logging.warning(f'AirflowScheduler executor {executorId} lost')

    def slaveLost(self, driver, slaveId):
        logging.warning(f'AirflowScheduler slave {slaveId} lost')

    def error(self, driver, message):
        logging.error(f'AirflowScheduler driver aborted {message}')
        raise AirflowException(f'AirflowScheduler driver aborted {message}')

    def resourceOffers(self, driver, offers):
        # logging.debug(f'got {len(offers)} offers')
        # logging.debug(f'task_counter: {self.task_counter}')
        # logging.debug(f'task_key_map: {self.task_key_map}')
        # logging.debug(f'task_queue.qsize: {self.task_queue.qsize()}')

        for offer in offers:
            # logging.debug(f'offer: {offer}')
            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == 'cpus':
                    offerCpus += resource.scalar.value
                elif resource.name == 'mem':
                    offerMem += resource.scalar.value
            offerOrgIds = [attr.text.value for attr in offer.attributes if attr.name == 'organizationId']

            # logging.info(f'Received offer {offer.id.value} with cpus={offerCpus}, mem={offerMem}, hostname={offer.url.address.hostname}, ip={offer.url.address.ip}')

            remainingCpus = offerCpus
            remainingMem = offerMem

            if self.task_queue.empty():
                logging.debug('task_queue is empty')

            # while (not self.task_queue.empty()):
            # while ((not self.task_queue.empty()) and remainingCpus >= self.task_cpu and remainingMem >= self.task_mem):
            # while (not self.task_queue.empty() and \
            #        remainingCpus >= self.task_cpu and \
            #        remainingMem >= self.task_mem):
            while all([
                not self.task_queue.empty(),
                remainingCpus >= self.task_cpu,
                remainingMem >= self.task_mem,
            ]):
            # while all([not self.task_queue.empty(), remainingCpus >= self.task_cpu, remainingMem >= self.task_mem]):
                # key, cmd, task_instance = self.task_queue.get()
                key, cmd = self.task_queue.get()
                # print('* getting task from queue', key, cmd)

                # validate resource offers

                # if not offer_suitable(task_instance, remainingCpus, remainingMem, offerOrgIds):
                # if not offer_suitable(remainingCpus, remainingMem):
                #     # if not suitable, put task back on the queue
                #     logging.info("offer not suitable for {}".format(key))
                #     # logging.info("task_instance.resources={}".format(task_instance.resources))
                #     logging.info("putting {} back into task_queue".format(key))
                #     # self.task_queue.put((key, cmd, task_instance))
                #     self.task_queue.put((key, cmd))
                #     break

                if 'astronomer' not in offerOrgIds:
                    print('skipping node - org id mismatch', offerOrgIds)
                    self.task_queue.put((key, cmd))
                    break


                # temporary hot fix for avoiding a bad node on staging with a full disk (TODO: remove this before prod)
                if offer.url.address.hostname == '10.200.0.102' or offer.url.address.ip == '10.200.0.102':
                    print(f'* not assigning to node {offer.url.address.hostname} {offer.url.address.ip}')
                    self.task_queue.put((key, cmd))
                    break

                tid = self.task_counter
                self.task_counter += 1
                # self.task_key_map[str(tid)] = (key, cmd, task_instance)
                self.task_key_map[str(tid)] = (key, cmd)

                logging.info(f'Launching task {tid} using offer {offer.id.value}')

                task = Dict()
                task.task_id.value = str(tid)
                task.agent_id.value = offer.agent_id.value
                # task.name = "AirflowTask %d" % (tid,)
                task_prefix = get_task_prefix()
                task.name = f'{task_prefix} {tid}'
                task.resources = [
                    {
                        'name': 'cpus',
                        'type': 'SCALAR',
                        'scalar': {
                            'value': self.task_cpu,
                        },
                    },
                    {
                        'name': 'mem',
                        'type': 'SCALAR',
                        'scalar': {
                            'value': self.task_mem,
                        },
                    },
                ]

                container = Dict()
                container.type = 'DOCKER'
                container.volumes = [
                    {
                        'host_path': '/airflow_home/logs',
                        'container_path': '/airflow_home/logs',
                        'mode': 'RW',
                    },
                    {
                        'host_path': '/var/run/docker.sock',
                        'container_path': '/var/run/docker.sock',
                        'mode': 'RW',
                    },
                ]

                docker = Dict()
                docker.image = os.getenv('DOCKER_AIRFLOW_IMAGE_TAG')
                docker.force_pull_image = True
                # docker.network = "BRIDGE"
                task_prefix = get_task_prefix()
                docker.parameters = [
                    {
                        'key': 'label',
                        'value': f'ORGANIZATION_ID={task_prefix}',
                    },
                ]

                container.docker = docker
                task.container = container

                command = Dict()
                command.value = cmd

                # Copy some environment vars from scheduler to execution docker container
                copy_env_var(command, 'AIRFLOW__CORE__SQL_ALCHEMY_CONN')
                copy_env_var(command, 'AIRFLOW_EMAIL_ON_FAILURE')
                copy_env_var(command, 'AIRFLOW__SMTP__SMTP_HOST')
                copy_env_var(command, 'AIRFLOW__SMTP__SMTP_STARTTLS')
                copy_env_var(command, 'AIRFLOW__SMTP__SMTP_SSL')
                copy_env_var(command, 'AIRFLOW__SMTP__SMTP_USER')
                copy_env_var(command, 'AIRFLOW__SMTP__SMTP_PORT')
                copy_env_var(command, 'AIRFLOW__SMTP__SMTP_PASSWORD')
                copy_env_var(command, 'AIRFLOW__SMTP__SMTP_MAIL_FROM')
                copy_env_var(command, 'AWS_ACCESS_KEY_ID')
                copy_env_var(command, 'AWS_SECRET_ACCESS_KEY')

                copy_env_var(command, 'SENTRY_ENABLED')
                copy_env_var(command, 'SENTRY_DSN')
                copy_env_var(command, 'AWS_S3_CLICKSTREAM_BUCKET')
                copy_env_var(command, 'CLICKSTREAM_BATCH_IMAGE')
                copy_env_var(command, 'REDSHIFT_POOL_SLOTS')
                copy_env_var(command, 'MONGO_URL')
                copy_env_var(command, 'PASSPHRASE')

                # these are in the saas executor but not this one
                # copy_env_var(command, "S3_ARTIFACT_PATH")
                # copy_env_var(command, "AIRFLOW_CONN_S3_LOGS")
                # copy_env_var(command, "AIRFLOW__CORE__REMOTE_LOG_CONN_ID")
                # copy_env_var(command, "AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER")

                # copy_env_var(command, "PYTHONPATH")

                task.command = command
                tasks.append(task)

                # remainingCpus -= task_instance.resources.cpu.value
                # remainingMem -= task_instance.resources.ram.value
                remainingCpus -= self.task_cpu
                remainingMem -= self.task_mem

            # driver.launchTasks(offer.id, tasks)
            if len(tasks) > 0:
                logging.info(f'Offer {offer.id.value} is launching tasks: {tasks}')
                driver.launchTasks(offer.id, tasks)
            else:
                print(f'declining offer {offer.id} - no tasks')
                driver.declineOffer(offer.id)


    def statusUpdate(self, driver, update):
        logging.info(f'Task {update.task_id.value} is in state {update.state}, data {update}')

        try:
            # key, cmd, task_instance = self.task_key_map[update.task_id.value]
            key, cmd = self.task_key_map[update.task_id.value]
        except KeyError:
            # The map may not contain an item if the framework re-registered after a failover.
            # Discard these tasks.
            logging.warn(f'Unrecognised task key {update.task_id.value}')
            return

        # XXX: Sometimes we get into a situation where task_queue.task_done()
        # throws errors. Could be due to some unhandled event we should be taking
        # care of somewhere else. Less likely, could be due to an issue where Queue.put isn't
        # properly locking.  Either way, just ignore for now.
        try:
            # TODO: why not if else here?
            if update.state == 'TASK_FINISHED':
                print('* finished', key, cmd)
                self.result_queue.put((key, State.SUCCESS))
                self.task_queue.task_done()
                del self.task_key_map[update.task_id.value]
                return

            # TODO: why not enum here for task states?
            # if update.state == "TASK_LOST" or \
            #    update.state == "TASK_KILLED" or \
            #    update.state == "TASK_FAILED":

            if update.state in ('TASK_LOST', 'TASK_KILLED', 'TASK_FAILED'):
                print(f'* {update.state} {key} {cmd}')
                self.result_queue.put((key, State.FAILED))
                self.task_queue.task_done()
                return

            if update.state == 'TASK_ERROR':
                print('* error', key, cmd)
                # catch potential race condition between airflow and the rest of the
                # frameworks in mesos
                # potentially not needed depending on how the offers actually work
                # i.e. if offers are sent out to all frameworks at once or one by one
                if 'more than available' in update.message:
                    self.task_queue.task_done()
                    del self.task_key_map[update.task_id.value]
                    # self.task_queue.put((key, cmd, task_instance))
                    self.task_queue.put((key, cmd))
                else:
                    logging.info(f'unhandled TASK_ERROR state: {update.message}')
                    self.result_queue.put((key, State.FAILED))
                    self.task_queue.task_done()
                    del self.task_key_map[update.task_id.value]

        except ValueError:
            logging.warn('Error marking task_done')


class AstronomerMesosExecutor(BaseExecutor):

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
                logging.error('Expecting mesos master URL for mesos executor')
                raise AirflowException('mesos.master not provided for mesos executor')

            master = configuration.get('mesos', 'MASTER')

            framework.name = get_framework_name()

            if not configuration.get('mesos', 'TASK_CPU'):
                task_cpu = 1
            else:
                task_cpu = configuration.getfloat('mesos', 'TASK_CPU')

            if not configuration.get('mesos', 'TASK_MEMORY'):
                task_memory = 256
            else:
                task_memory = configuration.getfloat('mesos', 'TASK_MEMORY')

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

            logging.info(f'MesosFramework master={master}, name={framework.name}, checkpoint={framework.checkpoint}')

            if configuration.getboolean('mesos', 'AUTHENTICATE'):
                if not configuration.get('mesos', 'DEFAULT_PRINCIPAL'):
                    logging.error('Expecting authentication principal in the environment')
                    raise AirflowException('mesos.default_principal not provided in authenticated mode')
                if not configuration.get('mesos', 'DEFAULT_SECRET'):
                    logging.error('Expecting authentication secret in the environment')
                    raise AirflowException('mesos.default_secret not provided in authenticated mode')

                principal = configuration.get('mesos', 'DEFAULT_PRINCIPAL')
                secret = configuration.get('mesos', 'DEFAULT_SECRET')

                framework.principal = credential.principal

                self._mesos_driver = MesosSchedulerDriver(
                    AirflowMesosScheduler(self.task_queue, self.result_queue, task_cpu, task_memory),
                    framework,
                    master,
                    use_addict=True,
                    principal=principal,
                    secret=secret)
            else:
                framework.principal = 'Airflow'
                self._mesos_driver = MesosSchedulerDriver(
                    AirflowMesosScheduler(self.task_queue, self.result_queue, task_cpu, task_memory),
                    framework,
                    master,
                    use_addict=True)
        return self._mesos_driver

    def start(self):
        self.mesos_driver.start()

    def execute_async(self, key, command, queue=None):
        logging.info(f'placing task on queue: {key} {command}')
        # pickle_id = self.pickle_from_command(command)

        # task_instance = None

        # if pickle_id is not None:
        #     # dag_pickle = self.find_pickle(pickle_id)
        #     if dag_pickle is not None:
        #         dag = dag_pickle.pickle
        #         dag_id, task_id, execution_date = key
        #         task_instance = dag.get_task(task_id=task_id)
        #         logging.debug('Have matching task %s', task_instance)
        #         logging.debug('requires resources: %s', task_instance.resources)
        #     else:
        #         print('dag_pickle is none')
        # else:
        #     print('pickle_id is none')
        # query dag_pickle table with pickle from command
        # get the task matching key off dag.tasks
        # print('key =', key)
        # print('command =', command)
        # if task_instance is not None:
        #     print('task_instance =', task_instance)

        # self.task_queue.put((key, command, task_instance))
        self.task_queue.put((key, command))

    def sync(self):
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        self.task_queue.join()
        self.mesos_driver.stop()

    # def pickle_from_command(self, command):

    #     from airflow.bin.cli import get_parser
    #     parser = get_parser()
    #     strip_airflow = command[len('airflow '):]
    #     args = parser.parse_args(strip_airflow.split())
    #     if hasattr(args, 'pickle'):
    #         return args.pickle

    # def find_pickle(self, pickle_id):
    #     session = Session()
    #     print('session =', session)
    #     logging.debug('Loading pickle id {}'.format(pickle_id))
    #     # q = session.query(DagPickle)
    #     q = Session.query(DagPickle)
    #     print('q =', q)
    #     q = q.filter(DagPickle.id == pickle_id)
    #     for i in q:
    #         print('i =', i)
    #     q = q.first()
    #     print('q =', q)
    #     return q
