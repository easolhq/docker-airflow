"""
Clickstream content ingestion via S3 bucket wildcard key into Airflow.
"""
from urllib import quote_plus
import abc
import logging
import os

from airflow import DAG
from airflow.operators import S3ClickstreamKeySensor
from airflow.operators.dummy_operator import DummyOperator

from fn.func import F
import stringcase

# TODO: make these explicit relative imports
from utils.config import ClickstreamActivity
from utils.defaults import config_default_args
from utils.db import MongoClient
from utils.docker import create_linked_docker_operator_simple
from utils.redshift import build_dag_id
from utils.s3 import config_s3_new


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv('AWS_S3_CLICKSTREAM_BUCKET')
BATCH_PROCESSING_IMAGE = os.getenv('CLICKSTREAM_BATCH_IMAGE')  # TODO: what is this?
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY', '')
config_s3_new(AWS_KEY, AWS_SECRET)

# TODO: rework redshift config with ryan to come from mongo?
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DB = os.getenv('REDSHIFT_DB')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_SCHEMA = os.getenv('REDSHIFT_SCHEMA')

AIRFLOW_CLICKSTREAM_BATCH_POOL = os.getenv('AIRFLOW_CLICKSTREAM_BATCH_POOL')

default_args = config_default_args()

# TODO: Update Name and Version to check for exsitance and defalut to ''

# TODO: Update to pull redshift information from mongo instead of env


class ClickstreamEvents(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, workflow, dag, upstream_task):
        self.workflow = workflow
        self.dag = dag
        self.upstream_task = upstream_task

        path = 'clickstream-data/{}/'.format(workflow_id)
        path += '{date}/'  # TODO: what is this for?  seems like a mistake (no variable, no .format call)... where does it get filled in?
        self.path = path

        self._default_events = ['page', 'track', 'identify', 'group', 'screen', 'alias']

    @property
    def workflow_id(self):
        return self.workflow['_id']

    @property
    def default_events(self):
        return self._default_events

    @abc.abstractmethod
    def get_events(self):
        raise NotImplementedError

    def _create_events_branch(self, task_id):
        tables = self.get_events()
        tables_op = DummyOperator(task_id=task_id, dag=self.dag)
        tables_op.set_upstream(self.upstream_task)

        for table in tables:
            sensor = S3ClickstreamKeySensor(task_id='s3_clickstream_table_sensor_{}'.format(table), default_args=default_args, dag=self.dag, bucket_name=S3_BUCKET, bucket_key=self.path + table, timedelta=0, soft_fail=True, poke_interval=5, timeout=10)
            sensor.set_upstream(tables_op)

            activity = ClickstreamActivity(workflow_id=self.workflow_id, table_name=table, redshift_host=REDSHIFT_HOST, redshift_port=REDSHIFT_PORT, redshift_db=REDSHIFT_DB, redshift_user=REDSHIFT_USER, redshift_password=REDSHIFT_PASSWORD, redshift_schema=REDSHIFT_SCHEMA, temp_bucket=S3_BUCKET, name_ver=BATCH_PROCESSING_IMAGE)
            copy_task = create_linked_docker_operator_simple(dag=self.dag, activity=activity.serialize(), pool=AIRFLOW_CLICKSTREAM_BATCH_POOL)
            copy_task.set_upstream(sensor)

    def create_key_sensor(self):
        raise NotImplementedError

    def create_copy_operator(self):
        raise NotImplementedError

    def run(self):
        self.create_key_sensor()
        self.create_copy_operator()
        self.create_branch()


class DefaultClickstreamEvents(ClickstreamEvents):

    def get_events(self):
        """Return the set of default event names."""
        return self.default_events

    def create_branch(self):
        """create_default_events_branch"""
        # built-in event types
        self._create_events_branch(task_id='default_tables')


class CustomClickstreamEvents(ClickstreamEvents):

    def __init__(self, workflow):
        super(CustomClickstreamEvents, self).__init__(workflow)
        # self._all_events = workflow['all']
        self._all_events = workflow['tables']

    @property
    def all_events(self):
        return self._all_events

    def get_events(self):
        """Return the set of custom event names."""
        all_events = set(self.all_events)
        default_events = set(self.default_events)
        custom_events = list(all_events - default_events)
        return custom_events

    def create_branch(self):
        """create_custom_events_branch"""
        self._create_events_branch(task_id='event_tables')


def main():
    global default_args

    client = MongoClient()
    workflows = client.clickstream_configs()

    for workflow in workflows:
        default_args['app_id'] = workflow['_id']

        dag = DAG(dag_id=build_dag_id(workflow), default_args=default_args, schedule_interval='15 * * * *')
        globals()[workflow['_id']] = dag

        start = DummyOperator(task_id='start', dag=dag)

        default_events = DefaultClickstreamEvents(workflow=workflow, dag=dag, upstream_task=start)
        default_events.run()

        custom_events = CustomClickstreamEvents(workflow=workflow, dag=dag, upstream_task=start)
        custom_events.run()

    client.close()
    logger.info('Finished exporting clickstream DAGs.')


if __name__ == '__main__':
    main()
