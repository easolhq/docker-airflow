from airflow import DAG
from utils.db import MongoClient
from utils.create_dag import create_dag

# Query for all workflows.
print('Querying for cloud workflows.')
client = MongoClient()
workflows = client.workflow_configs()

for workflow in workflows:
    # Get the workflow id.
    id_ = workflow['_id']
    globals()[id_] = create_dag(workflow, dag_cls=DAG, dag_type='etl')

print('Finished exporting ETL DAG\'s.')
client.close()
