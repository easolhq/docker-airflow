from utils.dag_linker import link_operators
from airflow.operators import DummyOperator
from airflow import DAG
import datetime

now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
start_date = datetime.datetime(now.year, now.month, now.day, now.hour)

def test_link_operators():
    dag = DAG("test_dag", default_args={"start_date": start_date}, schedule_interval="@once")
    activity_list = [{"name": "test_1", "id": 23}, {"name": "test_2", "id": 25, "dependsOn": [23]}]
    tasks = [DummyOperator(task_id="foo", dag=dag), DummyOperator(task_id="bar", dag=dag)]
    link_operators(activity_list, tasks)
    # print(tasks[0]._upstream_task_ids)
