import logging

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from plugins.hooks import S3FileHook
from plugins.operators.common import config_s3

config_s3()


class S3FileKeySensor(BaseSensorOperator):
    """
    Detect an exact file path in S3.
    """

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self, bucket_key, bucket_name, *args, **kwargs):
        super(S3FileKeySensor, self).__init__(*args, **kwargs)
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
