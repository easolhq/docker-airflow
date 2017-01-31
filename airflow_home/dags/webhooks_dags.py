from airflow import DAG
from airflow.operators import AstronomerS3KeySensor
from airflow.operators import DummyOperator
from datetime import datetime, timedelta
from fn.func import F
import stringcase as case
import pymongo
import os
from utils.docker import create_docker_operator, create_linked_docker_operator

now = datetime.utcnow() - timedelta(hours=1)
start_date = datetime(now.year, now.month, now.day, now.hour)

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': start_date,
    'email': 'greg@astronomer.io',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Get mongo url.
mongo_url = os.getenv('MONGO_URL', '')

# Connect to mongo.
print('Connecting to mongodb.')
client = pymongo.MongoClient(mongo_url)

# Query for all webhooks.
print('Querying for cloud webhooks.')
webhooks = client.get_default_database().webhookConfigs.find({})

print('Found {count} webhooks.'.format(count=webhooks.count()))
for webhook in webhooks:
    # Get the webhook id.
    webhook_id = webhook['_id']

    # Get the name of the webhook.
    webhook_name = webhook['name'] if 'name' in webhook else None

    # Lower and snake case the name if we have one, else just id.
    name = webhook_id if not webhook_name else '{name}__webhooks__{id}'.format(
        id=webhook_id,
        name=case.snakecase(case.lowercase(webhook_name)))

    print('Building DAG: {name}.').format(name=name)

    # Airflow looks at module globals for DAGs, so assign each webhook to
    # a global variable using it's id.
    dag = globals()[webhook_id] = DAG(
        name,
        default_args=default_args,
        schedule_interval='*/15 * * * *')

    dummy = DummyOperator(
        task_id='start',
        dag=dag)

    # The params for the sensor.
    sensor_params = {'webhook_id': webhook_id}

    # Prefix to search for.
    prefix = 'webhooks/{{ params.webhook_id }}/{{ ts }}/{{ ds }}'

    # Check if we have any files.  Probes for a minute, every 15 seconds.
    sensor = AstronomerS3KeySensor(
        task_id='s3_webhooks_sensor',
        bucket_name=os.getenv('AWS_S3_TEMP_BUCKET'),
        bucket_key=prefix,
        params=sensor_params,
        soft_fail=True,
        poke_interval=15,
        timeout=60,
        dag=dag)

    sensor.set_upstream(dummy)

    # Merge command.
    merge_command = """
        '{}'
        '{\"prefix\":\"webhooks/{{ params.webhook_id }}/{{ ts }}/{{ ds }}\", \"bucket\":\"{{ params.bucket }}\", \"remote\":false }'
        '{{ ts }}'
    """
    # Merge command params.
    merge_params = {'webhook_id': webhook_id, 'bucket': os.getenv('AWS_S3_TEMP_BUCKET')}

    # Create docker container operator for s3_merge_source.
    merge = create_docker_operator(dag, 's3_merge_source', merge_command, merge_params, 's3-merge-source')

    merge.set_upstream(sensor)

    # Grab activity list.
    activity_list = webhook['activityList']

    # List to hold activities to setup dependencies after creation.
    tasks = map(F(create_linked_docker_operator, dag, activity_list, 's3_merge_source'), enumerate(activity_list))

    # Loop through tasks and set dependencies.
    # TODO: Support `dependsOn` for full blown dags in mongo.
    for i, current in enumerate(tasks):
        if (i == 0):
            current.set_upstream(merge)
        else:
            current.set_upstream(tasks[i - 1])
