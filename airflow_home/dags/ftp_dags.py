"""
FTP content ingestion via S3 bucket wildcard key into Airflow.
"""

from urllib import quote_plus
import datetime
import logging
import os

from airflow import DAG
from airflow.hooks.base_hook import CONN_ENV_PREFIX
from airflow.operators import (
    AstronomerS3GetKeyAction, AstronomerS3KeySensor, AstronomerS3WildcardKeySensor,
)

from fn.func import F
import pymongo
import stringcase

from utils.docker import create_linked_docker_operator
from utils.paths import ensure_trailing_slash

MONGO_URL = os.getenv('MONGO_URL', '')
S3_BUCKET = os.getenv('AWS_S3_TEMP_BUCKET')
aws_key = os.getenv('AWS_ACCESS_KEY_ID', '')
aws_secret = quote_plus(os.getenv('AWS_SECRET_ACCESS_KEY', ''))
os.environ[CONN_ENV_PREFIX + 'S3_CONNECTION'] = 's3://{aws_key}:{aws_secret}@S3'.format(aws_key=aws_key, aws_secret=aws_secret)

now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
start_date = datetime.datetime(now.year, now.month, now.day, now.hour)

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': start_date,
    'email': 'greg@astronomer.io',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

client = pymongo.MongoClient(MONGO_URL)

ftp_configs = client.get_default_database().ftpConfigs.find({})
logging.debug('Found {} ftpConfigs.'.format(ftp_configs.count()))

# one FTP config per workflow and each customer can have zero or more workflows
for ftp_config in ftp_configs:
    id_ = ftp_config['_id']
    config_name = stringcase.snakecase(ftp_config['name'].lower())
    path = ftp_config['path']
    schedule = ftp_config['schedule']
    poke_interval = int(ftp_config['pokeInterval'])
    timeout = int(ftp_config['timeout'])
    activity_list = ftp_config['activityList']

    dag_name = '{config_name}__ftp__{id}'.format(config_name=config_name, id=id_)
    logging.debug('Building DAG "{}"'.format(dag_name))

    dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule)
    globals()[id_] = dag

    file_dir, file_ext = os.path.splitext(path)

    # probe for file presence
    if file_ext == '':
        # wildcard paths (directories)
        path = ensure_trailing_slash(path)
        task_1_s3_sensor = AstronomerS3WildcardKeySensor(
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
        task_1_s3_sensor = AstronomerS3KeySensor(
            task_id='s3_ftp_config_sensor_file',
            bucket_name=S3_BUCKET,
            bucket_key=path,
            soft_fail=True,
            poke_interval=poke_interval,
            timeout=timeout,
            dag=dag,
        )

    # fetch file path into XCom
    task_2_s3_get = AstronomerS3GetKeyAction(
        bucket_name=S3_BUCKET,
        bucket_key=path,
        xcom_push=True,
        task_id='s3_ftp_config_get_key',
        dag=dag,
    )
    task_2_match = task_2_s3_get.set_upstream(task_1_s3_sensor)

    # schedule downstream activity dependencies
    tasks = map(
        F(create_linked_docker_operator, dag, activity_list, ''),
        enumerate(activity_list),
    )
    for i, current in enumerate(tasks):
        if (i == 0):
            current.set_upstream(task_2_s3_get)
        else:
            current.set_upstream(tasks[i - 1])

client.close()
