"""
Clickstream content ingestion via S3 bucket wildcard key into Airflow.
"""

# from urllib.parse import quote_plus
import abc
import datetime
import logging
import os
from decouple import config

from blackmagic.py import blackmagic
# from fn.func import F

from airflow import DAG
from airflow.models import Pool
from airflow.utils.db import provide_session
from airflow.operators import S3ClickstreamKeySensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from utils.config import ClickstreamActivity
from utils.db import MongoClient
from utils.docker import create_linked_docker_operator_simple
from utils.redshift import build_dag_id
from utils.s3 import config_s3_new

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SENTRY_ENABLED = config('SENTRY_ENABLED', cast=bool, default=True)

if SENTRY_ENABLED:
    from raven.handlers.logging import SentryHandler
    from raven.conf import setup_logging

    SENTRY_DSN = config('SENTRY_DSN')
    handler = SentryHandler(SENTRY_DSN)
    handler.setLevel(logging.ERROR)
    setup_logging(handler)

S3_BUCKET = config('AWS_S3_CLICKSTREAM_BUCKET')
BATCH_PROCESSING_IMAGE = config('CLICKSTREAM_BATCH_IMAGE')

REDSHIFT_POOL_SLOTS = config('REDSHIFT_POOL_SLOTS', default=5)

AWS_KEY = config('AWS_ACCESS_KEY_ID')
AWS_SECRET = config('AWS_SECRET_ACCESS_KEY')
config_s3_new(AWS_KEY, AWS_SECRET)

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': datetime.datetime(2017, 9, 5),
    'email': 'taylor@astronomer.io',
    'email_on_failure': True,
    'email_on_retry': False,

    'retries': 2,
    # 'retries': 0,  # FIXME: remove right before merge

    'retry_delay': datetime.timedelta(minutes=5),
    'app_id': None,
    'copy_table': None
}

astro_resources = {'organizationId': 'astronomer'}


class ClickstreamEvents(object):
    """Base class for sensing and processing clickstream events."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, workflow, dag, upstream_task):
        """Initialize the clickstream config params and built-in event types."""
        self.workflow = workflow
        self.app_id = workflow['application']
        self.config = workflow['config']
        self.connection = workflow['connection']
        self.dag = dag
        self.upstream_task = upstream_task
        # self.details = details
        self._standard_events = ['page', 'track', 'identify', 'group', 'screen', 'alias']

    @property
    def workflow_id(self):
        """Get the clickstream config workflow ID."""
        # return self.workflow['_id']
        # return self.workflow['appId']
        return self.app_id

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

    def decrypt_connection(self):
        """
        Decrypt the Redshift connection details.

        For some reason [1][2][3][4] and potentially related [5], this method
        when converted into a function causes sensors to throw an obscure
        exception if details is passed into it as an arg / kwarg. The error
        occurs inside boto/connection.py in proxy_ssl and throws "TypeError: a
        bytes-like object is required, not 'str'." I'm not sure why yet but we
        should investigate more if it happens again. This is an open issue on
        boto.

        [1]: https://github.com/boto/boto/issues/3561
        [2]: https://github.com/boto/boto/pull/3699
        [3]: https://github.com/boto/boto/pull/2718
        [4]: https://github.com/boto/boto/pull/3695
        [5]: https://github.com/boto/boto/issues/2836
        """
        self.details = self.workflow['connection'][0]['details']
        if self.details['_encrypted'] is True:
            PASSPHRASE = config('PASSPHRASE')
            logger.info('* decrypting redshift config')

            try:
                decrypted = blackmagic.decrypt(passphrase=PASSPHRASE,
                                               obj=self.details)
                # print('PASSPHRASE =', PASSPHRASE)
                # print('decrypted =', decrypted)
            except Exception as e:
                logger.error(f'* blackmagic decrypt raised exception {e}')
                raise

            if not decrypted:
                logger.error('* blackmagic decrypt failed')
                raise Exception('cannot create copy operator without decrypted credentials')
            else:
                logger.info('* blackmagic decrypt succeeded')
                self.details = decrypted
        else:
            logger.info('* not decrypting redshift config')

    def _create_events_branch(self, task_id):
        """Create the DAG branch with sensor and operator (to be called by each subclass)."""
        self.decrypt_connection()
        tables = self.get_events()
        tables_op = DummyOperator(
            task_id=task_id,
            dag=self.dag,
            priority_weight=10,
            resources=astro_resources,
        )
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
            task_id=f's3_sensor_{table}',
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
            event_group=self.event_group_name,
            priority_weight=10,
            resources=astro_resources,
        )
        return sensor

    def create_copy_operator(self, table):
        """Create the copy task."""
        details = self.details

        host = details['host']
        port = details['port']
        user = details['user']
        pw = details['pw']
        db = self.workflow['config']['db']
        schema = self.workflow['config']['schema']
        encrypted = details['_encrypted']

        activity = ClickstreamActivity(
            workflow_id=self.workflow_id,
            table_name=table,
            redshift_host=host,
            redshift_port=port,
            redshift_db=db,
            redshift_schema=schema,
            redshift_user=user,
            redshift_password=pw,
            redshift_encrypted=encrypted,
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

            # this makes a massive performance difference on mesos
            # force_pull=False,
            force_pull=True,

            pool=self.workflow['pool'],

            # TODO: it's a pain to pass this all the way through... maybe fix
            # priority_weight=10,

            resources=astro_resources,
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


def get_redshift_pool_name(dag_id, slots):
    return f'redshift_loader_{dag_id}_{slots}'


@provide_session
def ensure_redshift_pool(name, slots, session=None):
    """
    Create Redshift connection pool dynamically.

    Currently this is one pool per DAG.
    """
    pool = Pool(pool=name, slots=slots)
    pool_query = (
        session.query(Pool)
        .filter(Pool.pool == name, Pool.slots == slots)
    )
    pool_query_result = pool_query.one_or_none()
    if not pool_query_result:
        logger.info(f'redshift pool "{name}" does not exist - creating it')
        session.add(pool)
        session.commit()
        logger.info(f'created redshift pool "{name}"')
    else:
        logger.info(f'redshift pool "{name}" already exists')


# def decrypt_connection(
#     details,
#     **kwargs,
# ):
#     """
#     Decrypt the Redshift connection details.

#     For some reason [1][2][3][4] and potentially related [5], this method
#     when converted into a function causes sensors to throw an obscure
#     exception if details is passed into it as an arg / kwarg. The error
#     occurs inside boto/connection.py in proxy_ssl and throws "TypeError: a
#     bytes-like object is required, not 'str'." I'm not sure why yet but we
#     should investigate more if it happens again. This is an open issue on
#     boto.

#     [1]: https://github.com/boto/boto/issues/3561
#     [2]: https://github.com/boto/boto/pull/3699
#     [3]: https://github.com/boto/boto/pull/2718
#     [4]: https://github.com/boto/boto/pull/3695
#     [5]: https://github.com/boto/boto/issues/2836
#     """
#     # self.details = self.workflow['connection'][0]['details']
#     # if self.details['_encrypted'] is True:
#     if details['_encrypted'] is True:
#         PASSPHRASE = config('PASSPHRASE')
#         logger.info('* decrypting redshift config')

#         try:
#             # decrypted = blackmagic.decrypt(passphrase=PASSPHRASE, obj=self.details)
#             decrypted = blackmagic.decrypt(passphrase=PASSPHRASE, obj=details)
#             # print('PASSPHRASE =', PASSPHRASE)
#             # print('decrypted =', decrypted)
#         except Exception as e:
#             logger.error('* blackmagic decrypt raised exception {}'.format(e))
#             raise

#         if not decrypted:
#             logger.error('* blackmagic decrypt failed')
#             raise Exception('cannot create copy operator without decrypted credentials')
#         else:
#             logger.info('* blackmagic decrypt succeeded')
#             details = decrypted
#     else:
#         logger.info('* not decrypting redshift config')

#     # print('details =', details)

#     return json.dumps({
#         'details': details,
#         'ti': kwargs['task_instance'],
#     })


def main():
    """Create clickstream DAG with branches for clickstream events grouped by type."""
    global default_args

    client = MongoClient()
    workflows = client.clickstream_configs()
    client.close()

    for workflow in workflows:
        default_args['app_id'] = workflow['_id']

        pool_name = get_redshift_pool_name(
            dag_id=workflow['_id'],
            slots=REDSHIFT_POOL_SLOTS,
        )
        workflow['pool'] = pool_name

        dag = DAG(
            dag_id=build_dag_id(workflow),
            default_args=default_args,
            schedule_interval='15 * * * *',
            # catchup=False,  # TODO: should i make this true (and adjust start date for prod?)
            catchup=True,
        )
        globals()[workflow['_id']] = dag

        start = DummyOperator(
            task_id='start',
            dag=dag,
            priority_weight=10,
            resources=astro_resources,
        )

        redshift_pool = PythonOperator(
            task_id='redshift_pool',
            dag=dag,
            python_callable=ensure_redshift_pool,
            op_kwargs={
                'name': pool_name,
                'slots': REDSHIFT_POOL_SLOTS,
            },

            # xcom_pull=True,  # remove - invalid
            # provide_context=True,

            priority_weight=10,
            resources=astro_resources,

            # execution_timeout=datetime.timedelta(minutes=5),
        )
        redshift_pool.set_upstream(start)

        standard = StandardClickstreamEvents(workflow=workflow, dag=dag,
                                             upstream_task=redshift_pool)
        standard.run()

        custom = CustomClickstreamEvents(workflow=workflow, dag=dag,
                                         upstream_task=redshift_pool)
        custom.run()

    logger.info('Finished exporting clickstream DAGs.')


main()
