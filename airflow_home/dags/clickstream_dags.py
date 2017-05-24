"""
Clickstream content ingestion via S3 bucket wildcard key into Airflow.
"""
from urllib.parse import quote_plus
import os
from utils.db import MongoClient
from utils.create_dag import create_dag
from utils.docker import create_linked_docker_operator

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    S3ClickstreamKeySensor
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import CONN_ENV_PREFIX

S3_BUCKET = os.getenv('AWS_S3_CLICKSTREAM_BUCKET')
BATCH_PROCESSING_IMAGE = os.getenv('CLICKSTREAM_BATCH_IMAGE')
aws_key = os.getenv('AWS_ACCESS_KEY_ID', '')
aws_secret = quote_plus(os.getenv('AWS_SECRET_ACCESS_KEY', ''))
os.environ[CONN_ENV_PREFIX + 'S3_CONNECTION'] = 's3://{aws_key}:{aws_secret}@S3'.format(aws_key=aws_key, aws_secret=aws_secret)

dag_args = {
    'retries': 0,
    'app_id': None,
    'copy_table': None
}

default_tables = [
    'page',
    'track',
    'identify',
    'group',
    'screen',
    'alias'
]


def create_branch(dag, parent_task, rs_connection, tables, delta, path):
    for table in tables:
        copy_sensor_task = S3ClickstreamKeySensor(
            task_id='s3_clickstream_table_sensor_%s' % (table),
            dag=dag,
            bucket_name=S3_BUCKET,
            bucket_key=path + table,
            timedelta=delta,
            soft_fail=True,
            poke_interval=5,
            timeout=10,
        )
        copy_sensor_task.set_upstream(parent_task)

        copy_task = create_linked_docker_operator(dag, [], '', (0, {
            'task_id': 's3_clickstream_table_copy_%s' % (table),
            'config': {
                'appId': workflow_id,
                'table': table,
                'redshift_host': rs_connection['host'],
                'redshift_port': rs_connection['port'],
                'redshift_db': rs_connection['db'],
                'redshift_user': rs_connection['user'],
                'redshift_password': rs_connection['pw'],
                'redshift_schema': rs_connection['schema'],
                'temp_bucket': S3_BUCKET,
                'timedelta': delta,
                '_encrypted': rs_connection['_encrypted']
            },
            'name': 'aries-activity-aries-base',
            'version': '0.1',
        }), '')
        copy_task.set_upstream(copy_sensor_task)


# Query for all workflows.
print('Querying for clickstream workflows.')
client = MongoClient()
workflows = client.clickstream_configs()

for workflow in workflows:
    # Get the workflow id.
    id_ = workflow['_id']
    workflow_id = workflow['appId']
    dag_args['app_id'] = workflow_id
    dag = create_dag(workflow, schedule_interval='15 * * * *', dag_cls=DAG, dag_type='clickstream', dag_args=dag_args)
    globals()[id_] = dag

    path = 'clickstream-data/{}/'.format(workflow_id)
    path += '{date}/'

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    def_tables = DummyOperator(
        task_id='default_tables',
        dag=dag
    )

    def_tables.set_upstream(start)

    event_tables = DummyOperator(
        task_id='event_tables',
        dag=dag
    )

    event_tables.set_upstream(start)

    connection = {
        "host": workflow['config']['host'],
        "port": workflow['config']['port'],
        "db": workflow['config']['db'],
        "_encrypted": workflow['config']['_encrypted'],
        "user": workflow['config']['user'],
        "pw": workflow['config']['pw'],
        "schema": workflow_id
    }

    create_branch(
        dag,
        def_tables,
        connection,
        default_tables,
        0,
        path)

    create_branch(
        dag,
        event_tables,
        connection,
        list(set(workflow['config']['tables']) - set(default_tables)),
        0,
        path
    )

client.close()
print('Finished exporting clickstream DAG\'s.')
