"""
AWS S3 utils.
"""

import os
from urllib.parse import quote_plus

from airflow.hooks.base_hook import CONN_ENV_PREFIX


def setenv(key, value):
    """Counterpart function to os.getenv."""
    os.environ[key] = value


def build_s3_url(aws_key, aws_secret):
    """Build S3 protocol connection URL."""
    return 's3://{key}:{secret}@S3'.format(key=aws_key, secret=aws_secret)


def config_s3_new(aws_key, aws_secret):
    """Configure S3_CONNECTION env var explicitly from args."""
    aws_secret = quote_plus(aws_secret)
    s3_url_key = CONN_ENV_PREFIX + 'S3_CONNECTION'
    s3_url = build_s3_url(aws_key, aws_secret)
    setenv(s3_url_key, s3_url)
