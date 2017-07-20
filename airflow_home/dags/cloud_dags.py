import logging
from decouple import config

from airflow import DAG
from utils.db import MongoClient
from utils.create_dag import create_dag

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SENTRY_ENABLED = config('SENTRY_ENABLED', cast=bool, default=True)

if SENTRY_ENABLED:
    from raven.handlers.logging import SentryHandler
    from raven.conf import setup_logging

    SENTRY_DSN = config('SENTRY_DSN')
    handler = SentryHandler(SENTRY_DSN)
    handler.setLevel(logging.ERROR)
    setup_logging(handler)
else:
    logger.warn("Not attaching sentry to cloud dags because sentry is disabled")

# Query for all workflows.
logger.info('Querying for cloud workflows.')
client = MongoClient()
workflows = client.workflow_configs()

for workflow in workflows:
    # Get the workflow id.
    id_ = workflow['_id']
    globals()[id_] = create_dag(workflow, dag_cls=DAG, dag_type='etl')

logger.info('Finished exporting ETL DAG\'s.')
client.close()
