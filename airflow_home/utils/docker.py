"""
DockerOperator wrappers and utils.
"""

import ast
from bson import json_util
import json
import os
from datetime import timedelta

from airflow.operators.docker_operator import DockerOperator
from utils.flatten import flatten_config


def trim_activity_name(name):
    """Trim 'aries-activity-' off."""
    return name[15:]


def format_task_name(name):
    """Format a task name for use as an Airflow task ID."""
    if name.startswith('aries-activity'):
        # TODO: legacy naming convention. remove once migrated
        return trim_activity_name(name)
    elif '/' in name:
        return name.split('/')[1]
    return name


def format_image_name(name, version):
    """Build Docker image name string."""
    # TODO: legacy naming convention. remove once migrated
    if name.startswith('aries-activity'):
        return 'astronomerio/{name}'.format(name=trim_activity_name(name))
    return '{name}:{version}'.format(name=name, version=version)


def create_docker_operator(params):
    """Create DockerOperator with default kwargs."""
    # Create defaults.
    defaults = {
        'remove': True,
        'xcom_push': True,
        'volumes': ['/var/log/filebeat:/usr/local/src/log']
    }

    # Merge params.
    docker_params = defaults.copy()
    docker_params.update(params)

    # Return a new DockerOperator.
    return DockerOperator(**docker_params)


def create_linked_docker_operator_simple(
        dag, activity,
        retries=None,
        retry_delay=None,
        force_pull=None,
        pool=None,
        resources=None):
    """
    Adapter to work around the tuple in called function signature.

    It's not possible to use full kwargs with a tuple arg; also, we don't use
    most of these args with Clickstream DAGs.
    """
    activity_tuple = (0, activity)
    return create_linked_docker_operator(
        dag, [], '', activity_tuple,
        retries=retries,
        retry_delay=retry_delay,
        force_pull=force_pull,
        pool=pool,
        resources=resources)


def create_linked_docker_operator(
        dag, activity_list, initial_task_id, activity_tuple,
        retries=None,
        retry_delay=None,
        force_pull=None,
        pool=None,
        resources=None):
    """
    Create DockerOperator for an activity that pulls XComs.

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
    command_params = {'config': config_str, 'prev_task_id': prev_task_id}

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
    # TODO: add ability to pass env vars down from the top and merge with some
    # defaults here
    env = {
        'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', ''),
        'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
        'AWS_REGION': os.getenv('AWS_REGION', ''),
        'AWS_S3_TEMP_BUCKET': os.getenv('AWS_S3_TEMP_BUCKET', ''),
        'ARIES_REMOVE_FILES_AFTER_TASK': 'TRUE',
        # 'AWS_S3_CLICKSTREAM_BUCKET': os.getenv('AWS_S3_CLICKSTREAM_BUCKET', ''),
        'S3_BUCKET': os.getenv('AWS_S3_CLICKSTREAM_BUCKET', ''),
        'SENTRY_ENABLED': os.getenv('SENTRY_ENABLED', True),
        'SENTRY_DSN': os.getenv('SENTRY_DSN'),
    }

    if force_pull is None:
        # Force pull in prod, use local in dev.
        force_pull = ast.literal_eval(os.getenv('FORCE_PULL_TASK_IMAGES', 'True'))

    # Create final dictionary for the DockerOperator
    params = {
        'params': command_params,
        'task_id': task_id,
        'pool': pool,
        'image': image_name,
        'environment': env,
        'privileged': privileged,
        'command': command,
        'resources': resources,
        'force_pull': force_pull,
        'execution_timeout': execution_timeout,
        'dag': dag
    }

    retries = dag.default_args.get('retries') if retries is None else retries
    if retries is not None:
        params['retries'] = retries

    retry_delay = dag.default_args.get('retry_delay') if retry_delay is None else retry_delay
    if retry_delay is not None:
        params['retry_delay'] = retry_delay

    # Return a new DockerOperator.
    return create_docker_operator(params)
