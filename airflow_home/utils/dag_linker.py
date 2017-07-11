import os
from plugins.operators.s3_remove_key_operator import S3RemoveKeyOperator
from .docker import create_linked_docker_operator, task_id_formatter

S3_BUCKET = os.getenv('AWS_S3_TEMP_BUCKET')


# 1. optimize depends_on/legacy check
def link_operators(activity_list, tasks):
    """Connects operators."""
    depends_on = depends_activities(activity_list)
    s3_remove_task = s3_key_retrieval(activity_list, tasks)

    # Legacy. Linear dag with key removal at the end
    if all(depends == 0 for depends in depends_on):
        for i, current in enumerate(tasks):
            if i > 0:
                current.set_upstream(tasks[i - 1])
            if i == len(tasks) - 1:
                current.set_downstream(s3_remove_task)
    else:
        # Maps activity id to associated task operator
        activity_operators = {
            activity['id']: tasks[idx] for idx, activity in enumerate(activity_list)
        }

        # Sets upstream dependencies
        for activity, task in zip(activity_list, tasks):
            deps_ops = [activity_operators[x] for x in activity.get('dependsOn', [])]
            task.set_upstream(deps_ops)

        # Finds leaf nodes and sets key removal downstream
        for task in tasks:
            if not task.downstream_list:
                task.set_downstream(s3_remove_task)


def depends_activities(activity_list):
    """Returns list of dependencies."""
    depends_on = []
    for activity in activity_list:
        if 'dependsOn' in activity:
            depends_on.append(activity['dependsOn'])
        else:
            depends_on.append(0)
    return depends_on


def s3_key_retrieval(activity_list, tasks):
    """ Returns S3 Key removal operator with task list dependencies"""
    bucket_keys = [
        task_id_formatter(index, activity_list[index]) for index, activity in enumerate(activity_list)
    ]
    script = """
        {{ task_instance.xcom_pull(task_ids=params.bucket_keys) }}
    """
    params = {'bucket_keys': bucket_keys}
    return S3RemoveKeyOperator(
        dag=tasks[0].dag,
        bucket_name=S3_BUCKET,
        bucket_keys=script,
        task_id='key_removal',
        params=params
    )
