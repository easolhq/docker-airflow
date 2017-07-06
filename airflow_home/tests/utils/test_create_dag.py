from airflow import DAG
from utils.create_dag import create_dag


def test_default_args_merge():
    workflow = {"name": "test", "activityList": [{"name": "activity_1"}]}
    dag_args = {"test_arg": "value"}
    dag = create_dag(workflow, dag_cls=DAG)
