"""
Clickstream content ingestion via S3 bucket wildcard key into Airflow.
"""

# from urllib.parse import quote_plus
import abc
import logging
import os

from airflow import DAG
from airflow.operators import S3ClickstreamKeySensor
from airflow.operators.dummy_operator import DummyOperator

# from fn.func import F

from utils.config import ClickstreamActivity
from utils.defaults import config_default_args
from utils.db import MongoClient
from utils.docker import create_linked_docker_operator_simple
from utils.redshift import build_dag_id
from utils.s3 import config_s3_new

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv('AWS_S3_CLICKSTREAM_BUCKET', default='astronomer-clickstream')
BATCH_PROCESSING_IMAGE = os.getenv('CLICKSTREAM_BATCH_IMAGE', default='astronomerio/py-aries-clickstream')
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY', '')
config_s3_new(AWS_KEY, AWS_SECRET)

AIRFLOW_CLICKSTREAM_BATCH_POOL = os.getenv('AIRFLOW_CLICKSTREAM_BATCH_POOL')

default_args = config_default_args()

# TODO: Update Name and Version to check for exsitance and defalut to ''


class ClickstreamEvents(object):
    """Base class for sensing and processing clickstream events."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, workflow, dag, upstream_task):
        """Initialize the clickstream config params and built-in event types."""
        self.workflow = workflow
        self.app_id = workflow['appId']
        self.config = workflow['config']
        self.dag = dag
        self.upstream_task = upstream_task
        self._standard_events = ['page', 'track', 'identify', 'group', 'screen', 'alias']

    @property
    def workflow_id(self):
        """Get the clickstream config workflow ID."""
        # return self.workflow['_id']
        return self.workflow['appId']

    @property
    def standard_events(self):
        """Get the clickstream built-in event types."""
        return self._standard_events

    @abc.abstractmethod
    def get_events(self):
        """Get the clickstream events relevant to the subclass (to be implemented in each subclass)."""
        raise NotImplementedError

    @abc.abstractproperty
    def event_group_name(self):
        raise NotImplementedError

    def _create_events_branch(self, task_id):
        """Create the DAG branch with sensor and operator (to be called by each subclass)."""
        tables = self.get_events()
        tables_op = DummyOperator(task_id=task_id, dag=self.dag)
        tables_op.set_upstream(self.upstream_task)

        for table in tables:
            sensor = self.create_key_sensor(table=table)
            sensor.set_upstream(tables_op)
            copy_task = self.create_copy_operator(table=table)
            if not copy_task:
                logger.info('Skipping table due to invalid config')
                continue
            copy_task.set_upstream(sensor)

    def create_key_sensor(self, table):
        """Create the S3 key sensor."""
        sensor = S3ClickstreamKeySensor(
            task_id='s3_clickstream_table_sensor_{}'.format(table),
            default_args=default_args,
            dag=self.dag,
            bucket_name=S3_BUCKET,
            workflow_id=self.workflow_id,
            app_id=self.app_id,
            table=table,
            timedelta=0,
            soft_fail=True,
            poke_interval=5,
            timeout=10,
            event_group=self.event_group_name
        )
        return sensor

    def create_copy_operator(self, table):
        """Create the copy task."""
        activity = ClickstreamActivity(
            workflow_id=self.workflow_id,
            table_name=table,
            redshift_host=self.config.get('host'),
            redshift_port=self.config.get('port'),
            redshift_db=self.config.get('db'),
            redshift_user=self.config.get('user'),
            redshift_password=self.config.get('pw'),
            redshift_encrypted=self.config.get('_encrypted'),
            temp_bucket=S3_BUCKET,
            name_ver=BATCH_PROCESSING_IMAGE,
            # name=BATCH_PROCESSING_IMAGE,
            # name_ver='DUMMY_VALUE:0.1',
        )

        if not activity.is_valid():
            return None

        copy_task = create_linked_docker_operator_simple(
            dag=self.dag,
            activity=activity.serialize(),
            pool=AIRFLOW_CLICKSTREAM_BATCH_POOL
        )
        return copy_task

    def run(self):
        """Run the tasks of this branch."""
        self.create_branch()


class StandardClickstreamEvents(ClickstreamEvents):
    """Concrete class for sensing and processing built-in clickstream events."""

    @property
    def event_group_name(self):
        return 'standard'

    def get_events(self):
        """Return the set of built-in event names."""
        return self.standard_events

    def create_branch(self):
        """Create the branch for built-in event types."""
        self._create_events_branch(task_id='default_tables')


class CustomClickstreamEvents(ClickstreamEvents):
    """Concrete class for sensing and processing custom clickstream events."""

    def __init__(self, workflow, *args, **kwargs):
        """Initialize the combined event list."""
        super(CustomClickstreamEvents, self).__init__(workflow, *args, **kwargs)
        self._all_events = self.config['tables']

    @property
    def all_events(self):
        """Return a list of all events."""
        return self._all_events

    @property
    def event_group_name(self):
        return 'custom'

    def get_events(self):
        """Return the set of custom event names."""
        all_events = set(self.all_events)
        standard_events = set(self.standard_events)
        custom_events = list(all_events - standard_events)
        return custom_events

    def create_branch(self):
        """Create the branch for custom event types."""
        self._create_events_branch(task_id='event_tables')


def main():
    """Create clickstream DAG with branches for clickstream events grouped by type."""
    global default_args

    client = MongoClient()
    workflows = client.clickstream_configs()

    for workflow in workflows:
        default_args['app_id'] = workflow['_id']

        dag = DAG(dag_id=build_dag_id(workflow), default_args=default_args, schedule_interval='@once') #15 * * * *
        globals()[workflow['_id']] = dag

        start = DummyOperator(task_id='start', dag=dag)

        standard_events = StandardClickstreamEvents(workflow=workflow, dag=dag, upstream_task=start)
        standard_events.run()

        custom_events = CustomClickstreamEvents(workflow=workflow, dag=dag, upstream_task=start)
        custom_events.run()

    client.close()
    logger.info('Finished exporting clickstream DAGs.')


main()
