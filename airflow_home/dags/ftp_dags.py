"""
FTP content ingestion via S3 bucket wildcard key into Airflow.
"""

from urllib.parse import quote_plus
import os
import logging
from decouple import config

from airflow import DAG
from airflow.hooks.base_hook import CONN_ENV_PREFIX
from airflow.operators import (
    S3FileKeySensor, S3WildcardKeySensor, S3GetKeyOperator,
)

from utils.paths import ensure_trailing_slash
from utils.db import MongoClient
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

S3_BUCKET = os.getenv('AWS_S3_TEMP_BUCKET')
aws_key = os.getenv('AWS_ACCESS_KEY_ID', '')
aws_secret = quote_plus(os.getenv('AWS_SECRET_ACCESS_KEY', ''))
os.environ[CONN_ENV_PREFIX + 'S3_CONNECTION'] = 's3://{aws_key}:{aws_secret}@S3'.format(aws_key=aws_key, aws_secret=aws_secret)

logger.info('Querying for ftpConfigs')
client = MongoClient()
ftp_configs = client.ftp_configs()

# one FTP config per workflow and each customer can have zero or more workflows
for ftp_config in ftp_configs:
    id_ = ftp_config['_id']
    dag = create_dag(ftp_config, dag_cls=DAG, dag_type='ftp')
    globals()[id_] = dag

    path = ftp_config['path']
    poke_interval = int(ftp_config['pokeInterval'])
    timeout = int(ftp_config['timeout'])

    file_dir, file_ext = os.path.splitext(path)

    # probe for file presence
    if file_ext == '':
        # wildcard paths (directories)
        path = ensure_trailing_slash(path)
        task_1_s3_sensor = S3WildcardKeySensor(
            task_id='s3_ftp_config_sensor_wildcard',
            bucket_name=S3_BUCKET,
            bucket_key=path,
            soft_fail=True,
            poke_interval=poke_interval,
            timeout=timeout,
            dag=dag,
        )
    else:
        # literal file paths
        task_1_s3_sensor = S3FileKeySensor(
            task_id='s3_ftp_config_sensor_file',
            bucket_name=S3_BUCKET,
            bucket_key=path,
            soft_fail=True,
            poke_interval=poke_interval,
            timeout=timeout,
            dag=dag,
        )

    # fetch file path into XCom
    task_2_s3_get = S3GetKeyOperator(
        bucket_name=S3_BUCKET,
        bucket_key=path,
        xcom_push=True,
        task_id='s3_ftp_config_get_key',
        dag=dag,
    )
    task_2_s3_get.set_upstream(task_1_s3_sensor)

    # TODO: Provide a better way to set upstream tasks
    # when true dags lands
    dag.tasks[0].set_upstream(task_2_s3_get)

logger.info('Finished exporting FTP DAGS')
client.close()
