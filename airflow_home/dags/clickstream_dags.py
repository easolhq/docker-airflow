"""
Clickstream content ingestion via S3 bucket wildcard key into Airflow.
"""

# TODO: clean up and group imports

from datetime import datetime, timedelta
from urllib import quote_plus
import os
from utils.db import MongoClient
from utils.docker import create_linked_docker_operator

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    S3ClickstreamKeySensor
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import CONN_ENV_PREFIX
from fn.func import F
import stringcase as case

# TODO: move logic into a main function

S3_BUCKET = os.getenv('AWS_S3_CLICKSTREAM_BUCKET')
BATCH_PROCESSING_IMAGE = os.getenv('CLICKSTREAM_BATCH_IMAGE')  # TODO: what is this?
aws_key = os.getenv('AWS_ACCESS_KEY_ID', '')
aws_secret = quote_plus(os.getenv('AWS_SECRET_ACCESS_KEY', ''))

os.environ[CONN_ENV_PREFIX + 'S3_CONNECTION'] = 's3://{aws_key}:{aws_secret}@S3'.format(aws_key=aws_key, aws_secret=aws_secret)

now = datetime.utcnow() - timedelta(days=1)
start_date = datetime(now.year, now.month, now.day, now.hour)

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': start_date,
    'email': 'greg@astronomer.io',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'app_id': None,
    'copy_table': None
}

# TODO: add alias type from segment spec?
default_tables = [
    'page',
    'track',
    'identify',
    'group',
    'screen'
]

# TODO: Update Name and Version to check for exsitance and defalut to ''

# TODO: Update to pull redshift information from mongo instead of env


def create_branch(dag, parent_task, tables, delta, path):
    """
    TODO
    """
    for table in tables:
        copy_sensor_task = S3ClickstreamKeySensor(
            task_id='s3_clickstream_table_sensor_%s' % (table),
            default_args=default_args,
            dag=dag,
            bucket_name=S3_BUCKET,
            bucket_key=path + table,
            timedelta=delta,
            soft_fail=True,
            poke_interval=5,
            timeout=10,
        )
        copy_sensor_task.set_upstream(parent_task)

        # TODO: separate retrieving these values into vars from the method call
        # TODO: rework this config with ryan to come from mongo

        copy_task = create_linked_docker_operator(dag, [], '', (0, {
            'task_id': 's3_clickstream_table_copy_%s' % (table),
            'config': {
                'appId': workflow_id,
                'table': table,
                'redshift_host': os.getenv('REDSHIFT_HOST'),
                'redshift_port': os.getenv('REDSHIFT_PORT'),
                'redshift_db': os.getenv('REDSHIFT_DB'),
                'redshift_user': os.getenv('REDSHIFT_USER'),
                'redshift_password': os.getenv('REDSHIFT_PASSWORD'),
                'redshift_schema': os.getenv('REDSHIFT_SCHEMA'),
                'temp_bucket': S3_BUCKET,
                'timedelta': delta
            },
            'name': '',  # BATCH_PROCESSING_IMAGE.split(':')[0],
            'version': '',  # BATCH_PROCESSING_IMAGE.split(':')[1]
        }), os.getenv('AIRFLOW_CLICKSTREAM_BATCH_POOL', None))  # TODO: remove the redundant None here
        copy_task.set_upstream(copy_sensor_task)


# TODO: switch print calls to logging

# TODO: wrap long method call arg lists

# Query for all workflows.
print('Querying for clickstream workflows.')
client = MongoClient()
workflows = client.clickstream_configs()

for workflow in workflows:
    # Get the workflow id.
    workflow_id = workflow['_id']
    default_args['app_id'] = workflow_id

    # Get the name of the workflow.
    # TODO use .get
    workflow_name = workflow['name'] if 'name' in workflow else 'astronomer_clickstream_to_redshift'

    name = '{name}__etl__{id}'.format(
        id=workflow_id,
        name=case.snakecase(case.lowercase(workflow_name))
    )

    print('Building DAG: {name}.').format(name=name)

    path = 'clickstream-data/{}/'.format(workflow_id)
    # TODO: what is this for?  seems like a mistake (no variable, no .format call)... where does it get filled in?
    path += '{date}/'

    # Airflow looks at module globals for DAGs, so assign each workflow to
    # a global variable using it's id.
    dag = globals()[workflow_id] = DAG(
        name,
        default_args=default_args,
        schedule_interval='*/15 * * * *')

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    s3_sensor = S3ClickstreamKeySensor(
        task_id='s3_clickstream_sensor',
        default_args=default_args,
        bucket_name=S3_BUCKET,
        bucket_key=path,
        soft_fail=False,
        poke_interval=5,
        timeout=10,
        dag=dag,
    )
    s3_sensor.set_upstream(start)
    create_branch(dag, s3_sensor, default_tables, 0, path)

# TODO: We should be able to remove the delayed_key_sensor because we are running at a delay

    s3_delayed_sensor = S3ClickstreamKeySensor(
        task_id='s3_clickstream_delayed_sensor',
        default_args=default_args,
        bucket_name=S3_BUCKET,
        bucket_key=path,
        timedelta=15,
        soft_fail=False,
        poke_interval=5,
        timeout=10,
        dag=dag,
    )
    s3_delayed_sensor.set_upstream(start)
    create_branch(
        dag,
        s3_delayed_sensor,
        list(set(workflow['tables']) - set(default_tables)),  # TODO: break this call out (isolates only custom events)
        15,
        path
    )

client.close()
print 'Finished exporting clickstream DAG\'s.'
