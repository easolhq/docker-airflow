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
    mesos_offer = Dict()
    mesos_offer.attributes = []
    mesos_offer.resources = []
    operator = BaseOperator(task_id='123')
    suitable = offer_suitable(mesos_offer, operator)
    assert suitable is False


def test_full_offer_suitable():
    mesos_offer = Dict()
    mesos_offer.resources = [
        Dict(name='cpus', type='SCALAR', scalar=Dict(value=1)),
        Dict(name='mem', type='SCALAR', scalar=Dict(value=512)),
        Dict(name='disk', type='SCALAR', scalar=Dict(value=512))
    ]
    mesos_offer.attributes = []
    operator = BaseOperator(task_id='123')
    suitable = offer_suitable(mesos_offer, operator)
    assert suitable is True


def test_offer_with_multiple_matches_against_resources():
    mesos_offer = Dict()
    mesos_offer.resources = [
        Dict(name='cpus', type='SCALAR', scalar=Dict(value=0.5)),
        Dict(name='cpus', type='SCALAR', scalar=Dict(value=0.5)),
        Dict(name='mem', type='SCALAR', scalar=Dict(value=512)),
        Dict(name='disk', type='SCALAR', scalar=Dict(value=512))
    ]
    mesos_offer.attributes = []
    operator = BaseOperator(task_id='123')
    suitable = offer_suitable(mesos_offer, operator)
    assert suitable is True


def test_offer_against_missing_organizationId_fails():
    mesos_offer = Dict()
    mesos_offer.resources = [
        Dict(name='cpus', type='SCALAR', scalar=Dict(value=1)),
        Dict(name='disk', type='SCALAR', scalar=Dict(value=512)),
        Dict(name='mem', type='SCALAR', scalar=Dict(value=512)),
    ]
    mesos_offer.attributes = [
        Dict(name='organizationId', type='TEXT', text=Dict(value='12345'))
    ]
    operator = BaseOperator(task_id='123')
    suitable = offer_suitable(mesos_offer, operator)
    assert suitable is False
