import json
import os
import os.path
import logging

from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import CONN_ENV_PREFIX
from urllib import quote_plus

from plugins.operators.astro_s3_hook import S3FileHook

aws_key = os.getenv('AWS_ACCESS_KEY_ID', '')
aws_secret = quote_plus(os.getenv('AWS_SECRET_ACCESS_KEY', ''))
os.environ[CONN_ENV_PREFIX + 'S3_CONNECTION'] = 's3://{aws_key}:{aws_secret}@S3'.format(aws_key=aws_key, aws_secret=aws_secret)

is_dir = lambda key: key.endswith('/')


def build_xcom(path):
    """
    Construct the JSON object that downstream tasks expect from XCom.
    """
    logging.info('Pushing path "{}" to XCom'.format(path))
    return json.dumps({'input': {'key': path}})


class AstronomerS3KeySensor(BaseSensorOperator):
    """
    Detect an exact file path in S3.
    """

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self, bucket_key, bucket_name, *args, **kwargs):
        super(AstronomerS3KeySensor, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key

    def poke(self, context):
        hook = S3FileHook(s3_conn_id='S3_CONNECTION')
        full_url = "s3://" + self.bucket_name + "/" + self.bucket_key
        logging.info('Poking for key "{}"'.format(full_url))
        file_exists = hook.check_for_key(
            key=self.bucket_key,
            bucket_name=self.bucket_name,
        )

        if not file_exists:
            logging.warning('File does not exist at "{}"'.format(full_url))

        return file_exists


class AstronomerS3WildcardKeySensor(BaseSensorOperator):
    """
    Detect a wildcard filepath in a "directory" in S3.
    """

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self, bucket_key, bucket_name, *args, **kwargs):
        super(AstronomerS3WildcardKeySensor, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.soft_fail = False

    def poke(self, context):
        hook = S3FileHook(s3_conn_id='S3_CONNECTION')
        full_url = os.path.join('s3://', self.bucket_name, self.bucket_key, '*')
        logging.info('Poking for key "{}"'.format(full_url))

        # Directories without trailing slash are supported by FTP DAGs which
        # ensure a trailing slash exists before reaching this method. This is
        # to ensure consistency in future usage.
        if not is_dir(self.bucket_key):
            logging.warning('Invalid path "{}" - the wildcard key sensor requires a trailing slash'.format(self.bucket_key))
            return False

        file_found = hook.check_for_wildcard_key(
            wildcard_key=full_url,
            delimiter='/',
        )

        if not file_found:
            logging.warning('No file found at "{}"'.format(full_url))

        return file_found


class AstronomerS3GetKeyAction(BaseOperator):
    """
    Grab the top S3 key match.

    Support both literal file paths and directories as wildcards.
    """

    @apply_defaults
    def __init__(self, bucket_key, bucket_name, xcom_push=False, *args, **kwargs):
        super(AstronomerS3GetKeyAction, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key

    def execute(self, context):
        hook = S3FileHook(s3_conn_id='S3_CONNECTION')

        if is_dir(self.bucket_key):
            # a directory
            bucket_key_wildcard = os.path.join(self.bucket_key, '*')
            key = hook.get_wildcard_key(
                wildcard_key=bucket_key_wildcard,
                bucket_name=self.bucket_name,
                delimiter='/',
            )
        else:
            # a literal file
            key = hook.get_key(
                self.bucket_key,
                bucket_name=self.bucket_name,
            )

        return build_xcom(key.name)
