import os.path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.operators.common import config_s3
from plugins.hooks import S3FileHook

config_s3()


class S3RemoveKeyOperator(BaseOperator):
    """Removes upstream DAG keys"""
    template_fields = ['bucket_keys']

    @apply_defaults
    def __init__(self, bucket_keys, bucket_name, *args, **kwargs):
        super(S3RemoveKeyOperator, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        # bucket keys, xcom pull all formatted_task_ids
        # can be done with the jinja template in link_operators, just need list of task_ids (0_activity_name, 1_json-flatten, 2_redshift-sink, etc)
        self.bucket_keys = bucket_keys

    def execute(self, context):
        print('Executing S3RemoveKeyOperator')
        print(self.bucket_keys)
        # hook = S3FileHook(s3_conn_id='S3_CONNECTION')
        # for key in self.bucket_keys:
        #     hook.delete_key(key)
