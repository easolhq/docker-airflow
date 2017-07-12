import os
import datetime

import boa

from utils.docker import create_linked_docker_operator

now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
start_date = datetime.datetime(now.year, now.month, now.day, now.hour)

email = os.getenv('AIRFLOW_ALERTS_EMAIL', '')
email_on_failure = os.getenv('AIRFLOW_EMAIL_ON_FAILURE', 'False') == 'True'

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': start_date,
    'email': email,
    'email_on_failure': email_on_failure,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


def resources_for_workflow(workflow):
    organization = workflow.get('organization')
    if isinstance(organization, dict) and organization.get('vpc') is True:
        organizationId = str(organization.get('_id'))
        return dict(organizationId=organizationId)
    else:
        # all tasks default to run in astronomer vpc
        return dict(organizationId='astronomer')


def create_tasks(dag, workflow):
    activity_list = workflow.get('activityList', [])
    resources = resources_for_workflow(workflow)
    tasks = [create_linked_docker_operator(dag, activity_list, '', activity, resources=resources) for activity in enumerate(activity_list)]
    for i, current in enumerate(tasks):
        if (i > 0):
            current.set_upstream(tasks[i - 1])


def create_dag(workflow, schedule_interval=None, dag_cls=None, dag_type=None, dag_args=None):
    """
    Creates a DAG instance from a workflow-like dict.
    Workflow objects are expected to have a name, schedule,
    and activityList
    :param workflow: The dict describing the DAG to build
    :type workflow: dict
    :param schedule_interval: A fallback schedule if a workflow does not define
    its own
    :type schedule_interval: string
    :param dag_type: describes the type of DAG being built
    :type dag_type: string
    :return DAG
    """
    if not dag_cls:
        raise Exception('must pass DAG class to create_dag')
# TODO: Improve this
    if isinstance(dag_args, dict):
        args = {**default_args, **dag_args}
    else:
        args = default_args
    id_ = workflow.get('_id')
    workflow_name = boa.constrict(workflow.get('name', '').lower())
    schedule = workflow.get('schedule', schedule_interval)

    if dag_type is not None:
        dag_name = '{workflow_name}__{dag_type}__{id_}'.format(
            workflow_name=workflow_name,
            dag_type=dag_type,
            id_=id_)
    else:
        dag_name = '{workflow_name}__{id_}'.format(
            workflow_name=workflow_name,
            id_=id_)

    print('Building DAG: {name}'.format(name=dag_name))
    dag = dag_cls(dag_name, default_args=args, schedule_interval=schedule)
    create_tasks(dag, workflow)
    return dag
