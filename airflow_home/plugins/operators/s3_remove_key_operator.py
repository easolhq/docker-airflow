import os.path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.operators.common import config_s3
from plugins.hooks import S3FileHook
from plugins.operators.utils import parse_keys

config_s3()


class S3RemoveKeyOperator(BaseOperator):
    """Removes upstream DAG keys"""
    template_fields = ['bucket_keys']

    @apply_defaults
    def __init__(self, bucket_keys, bucket_name, *args, **kwargs):
        super(S3RemoveKeyOperator, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_keys = bucket_keys

    def execute(self, context):
        hook = S3FileHook(s3_conn_id='S3_CONNECTION')
        formatted_keys = parse_keys(self.bucket_keys)
        deleted_keys = hook.delete_s3_key(formatted_keys, self.bucket_name)
        logging.info('Removed activity S3 files')
