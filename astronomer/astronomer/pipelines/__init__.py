import os
import datetime
import json
import random
from abc import abstractproperty
from airflow import DAG
from airflow.operators import BaseOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.hooks import FacebookAdsHook
from airflow.utils.decorators import apply_defaults
from airflow.models import Connection
from airflow.utils.db import provide_session

now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
start_date = datetime.datetime(now.year, now.month, now.day, now.hour)
default_args = {
        'start_date': start_date
}

def addProp(json_str, prop_name, value):
    json_val = json.loads(json_str)
    json_val[prop_name] = value
    return json.dumps(json_val)

class FacebookAdsOperator(BaseOperator):
    def __init__(self, method='get_ad_ids', facebook_ads_conn_id='default_facebook_ads', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.facebook_ads_conn_id = facebook_ads_conn_id
    
    def execute(self, context):
        fb_hook = FacebookAdsHook(facebook_ads_conn_id=self.facebook_ads_conn_id)
        ad_accounts = fb_hook.get_ad_accounts()
        # return [ad.get_id() for account in ad_accounts for ad in account.get_ads()]
        # TODO: unmock
        return [str(random.randint(100000000, 999999999)) for _ in range(143)]


class Pipeline:
    def __init__(self, pipeline_id=None):
        self.pipeline_id = pipeline_id
    
    @abstractproperty
    def dag(self):
        return None

class FacebookAdAccountsToRedshiftSubDag(Pipeline):
    def __init__(self, pipeline_id, start_date=None, schedule_interval=None, facebook_ads_conn_id=None, redshift_conn_id=None, redshift_database=None):
        super().__init__(pipeline_id)
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.facebook_ads_conn_id = facebook_ads_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.redshift_database = redshift_database


    @provide_session
    def get_conn(self, conn_id, session=None):
        conn = (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .first()
        )
        return conn


    @property
    def dag(self):
        facebook_conn = self.get_conn(self.facebook_ads_conn_id)
        redshift_conn = self.get_conn(self.redshift_conn_id)
        extra = facebook_conn.extra
        extra_json = json.loads(extra)
        access_token = extra_json.get('access_token')
        filters = dict(addProp=addProp)
        dag = DAG(self.pipeline_id, schedule_interval=self.schedule_interval, start_date=start_date, user_defined_filters=filters)
        insight_fields = ['ad_id', 'adset_id', 'campaign_id', 'account_id', 'action_values', 'clicks', 'total_unique_actions']
        default_conf = dict(insightFields=insight_fields, method='getAdInsights', accessToken=access_token, requestsPerSecond=10)
        pipes = [
            ('age_gender', dict(breakdowns=['age', 'gender'])),
            ('device_platform', dict(breakdowns=['device_platform'])),
            ('region_country', dict(breakdowns=['region', 'country'])),
            ('no_breakdown', dict(breakdowns=[]))
        ]
        get_ad_ids = FacebookAdsOperator(
            task_id='get_ad_ids',
            facebook_ads_conn_id=self.facebook_ads_conn_id,
            dag=dag
        )
        for pipe in pipes:
            (table, conf) = pipe
            conf.update(default_conf)
            env = {
                    'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', ''),
                    'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
                    'AWS_REGION': os.getenv('AWS_REGION', ''),
                    'AWS_S3_TEMP_BUCKET': os.getenv('AWS_S3_TEMP_BUCKET', ''),
            }

            facebook_ads_command = """
                ''
                '{{ params.config | addProp('id', task_instance.xcom_pull(task_ids=params.prev_task_id)) }}'
                '{{ ts }}'
                '{{ next_execution_date.isoformat() if next_execution_date != None else '' }}'
            """

            facebook_ads_params = { 'config': json.dumps(conf), 'prev_task_id': 'get_ad_ids' }

            facebook_ads_source = DockerOperator(
                    task_id='facebook_ads_{}'.format(table),
                    command=facebook_ads_command,
                    image='astronomerio/facebook-ads-source:rate-limit',
                    environment=env,
                    params=facebook_ads_params,
                    xcom_push=True,
                    dag=dag
            )

            facebook_ads_source.set_upstream(get_ad_ids)

            command = """
                '{{ task_instance.xcom_pull(task_ids=params.prev_task_id) }}'
                '{{ params.config }}'
                '{{ ts }}'
                '{{ next_execution_date.isoformat() if next_execution_date != None else '' }}'
            """

            json_to_csv_params = { 'prev_task_id': 'facebook_ads_{}'.format(table), 'config': '{}' }
            json_to_csv = DockerOperator(
                task_id='json_flatten_{}'.format(table),
                command=command,
                image='astronomerio/json-flatten',
                environment=env,
                params=json_to_csv_params,
                xcom_push=True,
                dag=dag
            )

            json_to_csv.set_upstream(facebook_ads_source)
            schema_hint = [
                dict(name='ad_id', type='string'),
                dict(name='adset_id', type='string'),
                dict(name='campaign_id', type='string'),
                dict(name='account_id', type='string')
            ]
            redshift_conf = dict(schema='facebook_ads', table=table, schemaHint=schema_hint, json=True)
            redshift_conf['connection'] = dict(
                host=redshift_conn.host,
                port=redshift_conn.port,
                user=redshift_conn.login,
                password=redshift_conn.password,
                database=self.redshift_database
            )

            redshift_params = { 'prev_task_id': 'json_flatten_{}'.format(table), 'config': json.dumps(redshift_conf)}
            redshift_sink = DockerOperator(
                task_id='redshift_{}'.format(table),
                command=command,
                image='astronomerio/redshift-sink',
                environment=env,
                params=redshift_params,
                xcom_push=True,
                dag=dag
            )

            redshift_sink.set_upstream(json_to_csv)
        return dag

class FacebookAdAccountsToRedshift(Pipeline):
    """
    Creates a FacebookAdAccountsToRedshiftSubDag for every Connection
    of type facebook_ads
    """
    def __init__(self, pipeline_id, all=False, facebook_ads_conn_ids=None, redshift_conn_id='default_redshift', redshift_database=None):
        super().__init__(pipeline_id)
        self.all = all
        self.facebook_ads_conn_ids = facebook_ads_conn_ids
        self.redshift_conn_id = redshift_conn_id
        self.redshift_database = redshift_database

    @provide_session
    def get_facebook_ads_conns(self, session=None):
        if self.all:
            filter = Connection.conn_type == 'facebook_ads'
        else:
            filter = Connection.conn_id.in_(self.facebook_ads_conn_ids)
        conns = (
            session.query(Connection)
            .filter(filter)
            .all()
        )
        return conns

    @property
    def dag(self):
        # create a subdag for each connection
        dag = DAG(self.pipeline_id, schedule_interval='@daily', default_args=default_args)
        facebook_ads_conns = self.get_facebook_ads_conns()
        for conn in facebook_ads_conns:
            subdag_name = '{}.{}'.format(self.pipeline_id, conn.conn_id)
            facebook_ads_to_redshift_single = FacebookAdAccountsToRedshiftSubDag(
                subdag_name, 
                start_date=dag.start_date,
                schedule_interval=dag.schedule_interval,
                facebook_ads_conn_id=conn.conn_id,
                redshift_conn_id=self.redshift_conn_id,
                redshift_database=self.redshift_database
            )
            subdag = SubDagOperator(
                subdag=facebook_ads_to_redshift_single.dag,
                task_id=conn.conn_id,
                dag=dag
            )
        return dag