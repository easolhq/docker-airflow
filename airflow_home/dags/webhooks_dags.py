import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators import S3FileKeySensor
from utils.db import MongoClient
from utils.docker import create_docker_operator
from utils.create_dag import create_dag

# Query for all webhooks.
print('Querying for cloud webhooks.')
client = MongoClient()
webhooks = client.webhook_configs()

for webhook in webhooks:
    # Get the webhook id.
    id_ = webhook['_id']
    # Airflow looks at module globals for DAGs, so assign each webhook to
    # a global variable using it's id.
# TODO: do we need initial_task_id still
    dag = create_dag(webhook, schedule_interval='*/15 * * * *', dag_cls=DAG, dag_type='webhooks')
    globals()[id_] = dag

    dummy = DummyOperator(
        task_id='start',
        dag=dag)

    # The params for the sensor.
    sensor_params = {'webhook_id': id_}

    # Prefix to search for.
    prefix = 'webhooks/{{ params.webhook_id }}/{{ ts }}/{{ ds }}'

    # Check if we have any files.  Probes for a minute, every 15 seconds.
    sensor = S3FileKeySensor(
        task_id='s3_webhooks_sensor',
        bucket_name=os.getenv('AWS_S3_TEMP_BUCKET'),
        bucket_key=prefix,
        params=sensor_params,
        soft_fail=True,
        poke_interval=15,
        timeout=60,
        dag=dag)

    sensor.set_upstream(dummy)

    # Merge command.
    merge_command = """
        '{}'
        '{\"prefix\":\"webhooks/{{ params.webhook_id }}/{{ ts }}/{{ ds }}\", \"bucket\":\"{{ params.bucket }}\", \"remote\":false }'
        '{{ ts }}'
    """
    # Merge command params.
    merge_params = {'webhook_id': id_, 'bucket': os.getenv('AWS_S3_TEMP_BUCKET')}

    # Create docker container operator for s3_merge_source.
    merge = create_docker_operator({
        'task_id': 's3_merge_source',
        'image': 's3-merge-source',
        'command': merge_command,
        'params': merge_params,
        'dag': dag,
    })

    merge.set_upstream(sensor)

    dag.tasks[0].set_upstream(merge)

print('Finished exporting webhook DAGS')
client.close()
