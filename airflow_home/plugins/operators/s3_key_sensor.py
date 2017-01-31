import os.path
import logging

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from plugins.hooks import S3FileHook
from plugins.operators.common import config_s3
from plugins.operators.utils import is_dir

config_s3()


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


class S3WildcardKeySensor(BaseSensorOperator):
    """
    Detect a wildcard filepath in a "directory" in S3.
    """

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self, bucket_key, bucket_name, *args, **kwargs):
        super(S3WildcardKeySensor, self).__init__(*args, **kwargs)
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
