from urllib import quote_plus
import os

from airflow.hooks.base_hook import CONN_ENV_PREFIX


def config_s3():
    aws_key = os.getenv('AWS_ACCESS_KEY_ID', '')
    aws_secret = quote_plus(os.getenv('AWS_SECRET_ACCESS_KEY', ''))
    os.environ[CONN_ENV_PREFIX + 'S3_CONNECTION'] = 's3://{aws_key}:{aws_secret}@S3'.format(aws_key=aws_key, aws_secret=aws_secret)
