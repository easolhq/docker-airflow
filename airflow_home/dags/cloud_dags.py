from airflow import DAG
from datetime import datetime, timedelta
from fn.func import F
import stringcase as case
import os
from utils.docker import create_linked_docker_operator
from utils.db import MongoClient

now = datetime.utcnow() - timedelta(hours=1)
start_date = datetime(now.year, now.month, now.day, now.hour)

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': start_date,
    'email': 'greg@astronomer.io',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Query for all workflows.
print('Querying for cloud workflows.')
client = MongoClient()
workflows = client.workflow_configs()

for workflow in workflows:
    # Get the workflow id.
    workflow_id = workflow['_id']

    # Get the name of the workflow.
    workflow_name = workflow['name'] if 'name' in workflow else None

    # Lower and snake case the name if we have one, else just id.
    name = workflow_id if not workflow_name else '{name}__etl__{id}'.format(
        id=workflow_id,
        name=case.snakecase(case.lowercase(workflow_name)))

    print('Building DAG: {name}.').format(name=name)

    # Legacy.
    interval = timedelta(seconds=int(workflow['interval'])) if 'interval' in workflow else None
    # New.
    schedule = workflow['schedule'] if 'schedule' in workflow else None
    # Figure out which one to use.
    schedule_interval = schedule if schedule is not None else interval

    # Airflow looks at module globals for DAGs, so assign each workflow to
    # a global variable using it's id.
    dag = globals()[workflow_id] = DAG(
        name,
        default_args=default_args,
        schedule_interval=schedule_interval)

    # Grab activity list.
    activity_list = workflow['activityList']

    # List to hold activities to setup dependencies after creation.
    tasks = map(F(create_linked_docker_operator, dag, activity_list, ''), enumerate(activity_list))

    # Loop through tasks and set dependencies.
    # TODO: Support `dependsOn` for full blown dags in mongo.
    for i, current in enumerate(tasks):
        if (i > 0):
            current.set_upstream(tasks[i - 1])

client.close()
print('Finished exporting ETL DAG\'s.')
