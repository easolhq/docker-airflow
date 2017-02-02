import ast
import json
import os
from datetime import timedelta

from airflow.operators.docker_operator import DockerOperator


# Trim aries-activity- off.
def trim_activity_name(name):
    return name[15:]

def task_name(name):
    if 'aries-activity' in name:
# legacy naming convention. remove once migrated
        return trim_activity_name(name)
    return name.split('/')[1]

def get_image_name(name, version):
# legacy naming convention. remove once migrated
    if 'aries-activity' in name:
        return 'astronomerio/{name}'.format(**locals())
    return '{name}:{version}'.format(**locals())

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


def create_linked_docker_operator(dag, activity_list, initial_task_id, (index, activity)):
    # Get the previous tasks id for xcom.
    prev_task_id = (
        initial_task_id if index is 0
        else '{index}_{name}'.format(
            index=index-1,
            name=task_name(activity_list[index - 1]['name'])))

    # Template out a command.
    command = """
        '{{ task_instance.xcom_pull(task_ids=params.prev_task_id) }}'
        '{{ params.config }}'
        '{{ ts }}'
    """

    # Get config.
    config = activity['config'] if 'config' in activity else {}
    config_str = json.dumps(config)

    # The params for the command.
    params = {'config': config_str, 'prev_task_id': prev_task_id}

    # Format the image name.
    image_name = get_image_name(activity['name'], activity['version'])

    # Create task id.
    task_id = '{index}_{name}'.format(
            index=index,
            name=task_name(activity['name']))

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
        'ARIES_REMOVE_FILES_AFTER_TASK': 'TRUE'
    }

    # Force pull in prod, use local in dev.
    force_pull = ast.literal_eval(os.getenv('FORCE_PULL_TASK_IMAGES', 'True'))

    # Create final dictionary for the DockerOperator
    params = {
        'task_id': task_id,
        'image': image_name,
        'environment': env,
        'privileged': privileged,
        'command': command,
        'params': params,
        'force_pull': force_pull,
        'execution_timeout': execution_timeout,
        'dag': dag
    }

    # Return a new DockerOperator.
    return create_docker_operator(params)
