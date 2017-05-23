"""
TODO
"""

import logging
from datetime import timedelta

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from plugins.hooks import S3FileHook
from plugins.operators.common import config_s3

config_s3()


class S3ClickstreamKeySensor(BaseSensorOperator):
    """
    Detect an execution-date bound file path in S3.
    """

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self, bucket_key, bucket_name, timedelta=0, *args, **kwargs):
        """
        TODO
        """
        super(S3ClickstreamKeySensor, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.timedelta = timedelta

    def poke(self, context):
        """
        TODO
        """
        logging.info('Starting poke')
        hook = S3FileHook(s3_conn_id='S3_CONNECTION')
        # TODO: where does this context['ti'] get populated from?  is this task ID?
        execution_date = context['ti'].execution_date
        # TODO: does the datetime part here need to change since we're shifting the whole DAG back on delay?
        batch_datetime = execution_date - timedelta(
            minutes=((execution_date.minute % 15) + self.timedelta)
        )
        bucket_key = self.bucket_key.format(
            date=batch_datetime.strftime("%Y-%m-%dT%H_%M_%S")
        )
        full_url = 's3://' + self.bucket_name + '/' + bucket_key + '*'
        logging.info('Poking for key "{}"'.format(full_url))

        file_found = hook.check_for_wildcard_key(
            wildcard_key=full_url,
            delimiter='/',
        )

        if not file_found:
            logging.warning('No file found at "{}"'.format(full_url))

        return file_found
