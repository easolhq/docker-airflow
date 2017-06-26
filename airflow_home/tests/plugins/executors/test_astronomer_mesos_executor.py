from unittest.mock import patch
from queue import Queue
from addict import Dict
from airflow.models import BaseOperator
from plugins.executors.astronomer_mesos_executor import *


@patch('pymesos.MesosSchedulerDriver')
def test_scheduler_resourceOffers(driver):
    task_queue = Queue()
    result_queue = Queue()
    mesos_scheduler = AirflowMesosScheduler(task_queue, result_queue)
    offers = [Dict({
        "id": dict(value="1"),
        "resources": [
            dict(name="cpus", scalar={"value": 1}),
            dict(name="mem", scalar={"value": 1}),
        ]
    })]
    mesos_scheduler.resourceOffers(driver, offers)


@patch('pymesos.MesosSchedulerDriver')
def test_executor_fails_jobs(driver):
    executor = AstronomerMesosExecutor(driver)
    executor.start()


def test_copy_env_var():
    command = Dict()
    copy_env_var(command, 'TEST_VAR')
    assert command.environment.variables[0]['name'] == 'TEST_VAR'


def test_copy_env_var_merges():
    command = Dict()
    copy_env_var(command, 'TEST_VAR')
    copy_env_var(command, 'TEST_VAR2')
    assert command.environment.variables[0]['name'] == 'TEST_VAR'
    assert command.environment.variables[1]['name'] == 'TEST_VAR2'


def test_empty_offer_not_suitable():
    operator = BaseOperator(task_id='123', resources=dict(organizationId='astronomer'))
    suitable = offer_suitable(operator)
    assert suitable is False


def test_full_offer_suitable():
    cpus = 1
    mem = 512
    offerOrgIds = ['astronomer']
    operator = BaseOperator(task_id='123', resources=dict(organizationId='astronomer'))
    suitable = offer_suitable(operator, cpus, mem, offerOrgIds)
    assert suitable is True


def test_offer_against_missing_organizationId_fails():
    cpus = 1
    mem = 512
    operator = BaseOperator(task_id='123', resources=dict(organizationId='astronomer'))
    suitable = offer_suitable(operator, cpus, mem)
    assert suitable is False
