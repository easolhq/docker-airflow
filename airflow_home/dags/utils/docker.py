import ast
import json
import os

from airflow.operators.docker_operator import DockerOperator


# Trim aries-activity- off.
def trim_activity_name(name):
    return name[15:]


def create_docker_operator(dag, task_id, cmd, params, image_name, privileged=False):
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

    # Return a new docker operator with our command.
    return DockerOperator(
        task_id=task_id,
        image='astronomerio/{image_name}'.format(image_name=image_name),
        environment=env,
        remove=True,
        privileged=privileged,
        command=cmd,
        params=params,
        xcom_push=True,
        force_pull=force_pull,
        dag=dag)


def create_linked_docker_operator(dag, activity_list, initial_task_id, (index, activity)):
    # Get the previous tasks id for xcom.
    prev_task_id = (
        initial_task_id if index is 0
        else '{index}_{name}'.format(
            index=index-1,
            name=trim_activity_name(activity_list[index - 1]['name'])))

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

    # Get the activity name.
    activity_name = trim_activity_name(activity['name'])

    # Create task id.
    task_id = '{index}_{name}'.format(
            index=index,
            name=trim_activity_name(activity['name']))

    # check for vpnConnection. Must run privileged if a tunnel is needed
    privileged = 'vpnConnection' in config.get('connection', {})

    # Return the operator.
    return create_docker_operator(dag, task_id, command, params, activity_name, privileged)
