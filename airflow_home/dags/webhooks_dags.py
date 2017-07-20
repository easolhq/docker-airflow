import os
import logging
from decouple import config

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import S3FileKeySensor
from utils.db import MongoClient
from utils.docker import create_docker_operator
from utils.create_dag import create_dag

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SENTRY_ENABLED = config('SENTRY_ENABLED', cast=bool, default=True)

if SENTRY_ENABLED:
    from raven.handlers.logging import SentryHandler
    from raven.conf import setup_logging

    SENTRY_DSN = config('SENTRY_DSN')
    handler = SentryHandler(SENTRY_DSN)
    handler.setLevel(logging.ERROR)
    setup_logging(handler)
else:
    logger.warn("Not attaching sentry to cloud dags because sentry is disabled")

# Query for all webhooks.
logger.info('Querying for cloud webhooks.')
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

logger.info('Finished exporting webhook DAGS')
client.close()
