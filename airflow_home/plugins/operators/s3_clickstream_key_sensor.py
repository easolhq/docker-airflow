"""
Clickstream events file detector.
"""

import logging
from datetime import timedelta

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from plugins.hooks import S3FileHook
from plugins.operators.common import config_s3

config_s3()


class S3ClickstreamKeySensor(BaseSensorOperator):
    """Detect an execution-date bound file path in S3."""

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self, bucket_name, workflow_id, table, timedelta=0, *args, **kwargs):
        """Initialize sensor."""
        super(S3ClickstreamKeySensor, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.workflow_id = workflow_id
        self.table = table
        self.timedelta = timedelta

    def _build_s3_key(self, execution_date):
        """Generate the S3 key for this event table's current batch."""
        # TODO: does the datetime part here need to change since we're shifting the whole DAG back on delay?
        batch_datetime = execution_date - timedelta(minutes=15)
        key = 'clickstream-data/{workflow_id}/{date}/{table}'.format(
        batch_datetime_str = batch_datetime.strftime('%Y-%m-%dT%H')
            workflow_id=self.workflow_id,
            date=batch_datetime_str,
            table=self.table
        )
        return key

    def _build_s3_wildcard_url(self, key):
        """Create S3 URL for wildcard path."""
        url = 's3://{bucket}/{key}*'.format(bucket=self.bucket_name, key=key)
        return url

    def _build_url(self, context):
        """Build the full S3 URL."""
        task_instance = context['ti']
        s3_key = self._build_s3_key(execution_date=task_instance.execution_date)
        url = self._build_s3_wildcard_url(key=s3_key)
        logging.info('Poking for key "{}"'.format(url))
        return url

    def poke(self, context):
        """Poke for clickstream event files."""
        logging.info('Starting poke')
        hook = S3FileHook(s3_conn_id='S3_CONNECTION')
        full_url = self._build_url(context=context)
        file_found = hook.check_for_wildcard_key(wildcard_key=full_url, delimiter='/')

        if not file_found:
            logging.warning('No file found at "{}"'.format(full_url))

        return file_found
