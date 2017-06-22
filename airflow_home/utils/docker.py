"""
TODO
"""

import ast
from bson import json_util
import json
import os
from datetime import timedelta

from airflow.operators.docker_operator import DockerOperator
from utils.flatten import flatten_config


# Trim aries-activity- off.
def trim_activity_name(name):
    return name[15:]


# formats a task name for use as an airflow task id
def format_task_name(name):
    if name.startswith('aries-activity'):
        # TODO: legacy naming convention. remove once migrated
        return trim_activity_name(name)
    elif '/' in name:
        return name.split('/')[1]
    return name


# formats an image name
def format_image_name(name, version):
    # TODO: legacy naming convention. remove once migrated
    if name.startswith('aries-activity'):
        return 'astronomerio/{name}'.format(name=trim_activity_name(name))
    return '{name}:{version}'.format(name=name, version=version)


def create_docker_operator(params):
    # Create defaults.
    defaults = {
        'remove': True,
        'xcom_push': True,
        'volumes': ['/var/log/filebeat/aries:/usr/local/src/log']
    }

    # Merge params.
    docker_params = defaults.copy()
    docker_params.update(params)

    # Return a new DockerOperator.
    return DockerOperator(**docker_params)


def create_linked_docker_operator_simple(dag, activity, pool=None, resources=None):
    """
    Adapter to work around the tuple in called function signature.

    It's not possible to use full kwargs with a tuple arg; also, we don't use
    most of these args with Clickstream DAGs.
    """
    activity_tuple = (0, activity)
    return create_linked_docker_operator(dag, [], '', activity_tuple, pool, resources)


def create_linked_docker_operator(dag, activity_list, initial_task_id, activity_tuple, pool=None, resources=None):
    """
    Creates a DockerOperator from the activity in activity_tuple,
    configured to pull Xcoms from the previous task
    :param: activity_list: The full activity list for the workflow
    :type: activity_list: list
    :param initial_task_id: The id of the initial task
    :type initial_task_id: string
    :param resources: an instance of Resources. Defines resources for the operator when ran by the executor
    :type resources: Resources
    :param activity_tuple: A tuple consisting of the index and activity
    :type activity_tuple: tuple (index, activity)
    :return DockerOperator
    """
    index, activity = activity_tuple
    # Get the previous tasks id for xcom.
    prev_task_id = (
        initial_task_id if index is 0
        else '{index}_{name}'.format(
            index=index - 1,
            name=format_task_name(activity_list[index - 1]['name'])))

    # Template out a command.
    command = """
        '{{ task_instance.xcom_pull(task_ids=params.prev_task_id) }}'
        '{{ params.config }}'
        '{{ ts }}'
        '{{ next_execution_date.isoformat() if next_execution_date != None else '' }}'
    """

    # Get config.
    config = activity['config'] if 'config' in activity else {}
    flattened_config = flatten_config(config)
    config_str = json.dumps(flattened_config, default=json_util.default)

    # The params for the command.
    params = {'config': config_str, 'prev_task_id': prev_task_id}

    # Format the image name.
    version = activity['version'] if 'version' in activity else 'latest'
    image_name = format_image_name(activity['name'], version)

    # Create task id.
    # TODO: use activity.get('task_id', '...') instead
    task_id = activity['task_id'] if 'task_id' in activity else '{index}_{name}'.format(
        index=index,
        name=format_task_name(activity['name']))

    # Check for vpnConnection. Must run privileged if a tunnel is needed.
    privileged = 'vpnConnection' in config.get('connection', {})

    # Check for an optional execution timeout in minutes.
    timeout = config.get('executionTimeout', None)
    execution_timeout = timedelta(minutes=timeout) if timeout is not None else None

    # Pass some env vars through.
    env = {
        'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', ''),
        'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
        'AWS_REGION': os.getenv('AWS_REGION', ''),
        'AWS_S3_TEMP_BUCKET': os.getenv('AWS_S3_TEMP_BUCKET', ''),
        'AWS_S3_CLICKSTREAM_BUCKET': os.getenv('AWS_S3_CLICKSTREAM_BUCKET', '')
    }

    # Force pull in prod, use local in dev.
    force_pull = ast.literal_eval(os.getenv('FORCE_PULL_TASK_IMAGES', 'True'))

    # Create final dictionary for the DockerOperator
    params = {
        'task_id': task_id,
        'pool': pool,
        'image': image_name,
        'environment': env,
        'privileged': privileged,
        'command': command,
        'params': params,
        'resources': resources,
        'force_pull': force_pull,
        'execution_timeout': execution_timeout,
        'dag': dag
    }

    # Return a new DockerOperator.
    return create_docker_operator(params)
