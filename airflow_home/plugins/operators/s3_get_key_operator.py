import os.path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from plugins.hooks import S3FileHook
from plugins.operators.common import config_s3
from plugins.operators.utils import build_xcom, is_dir

config_s3()


class S3GetKeyOperator(BaseOperator):
    """
    Grab the top S3 key match.

    Support both literal file paths and directories as wildcards.
    """

    @apply_defaults
    def __init__(self, bucket_key, bucket_name, xcom_push=False, *args, **kwargs):
        super(S3GetKeyOperator, self).__init__(*args, **kwargs)
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
