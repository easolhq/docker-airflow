from urllib import quote_plus
import os

from airflow.hooks.base_hook import CONN_ENV_PREFIX


def setenv(key, value):
    """Counterpart function to os.getenv."""
    os.environ[key] = value


def build_s3_url(aws_key, aws_secret):
    return 's3://{key}:{secret}@S3'.format(key=aws_key, secret=aws_secret)


def config_s3_new(aws_key, aws_secret):
    """
    Configure S3 directly from args (as opposed to environment variables
    behind the scenes).
    """
    aws_secret = quote_plus(aws_secret)
    s3_url_key = CONN_ENV_PREFIX + 'S3_CONNECTION'
    s3_url = build_s3_url(aws_key, aws_secret)
    setenv(s3_url_key, s3_url)
