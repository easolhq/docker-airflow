from utils.dag_linker import link_operators
from airflow.operators import DummyOperator
from airflow import DAG
from plugins.operators.s3_remove_key_operator import S3RemoveKeyOperator
import datetime

now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
start_date = datetime.datetime(now.year, now.month, now.day, now.hour)


def test_link_operators_no_dependencies():
    dag = DAG("test_dag", default_args={"start_date": start_date}, schedule_interval="@once")
    activity_list = [
        {"name": "test_1", "_id": 23},
        {"name": "test_2", "_id": 25}
    ]
    tasks = [
        DummyOperator(task_id="source", dag=dag),
        DummyOperator(task_id="sink", dag=dag)
    ]
    link_operators(activity_list, tasks)
    assert "source" in tasks[1]._upstream_task_ids


def test_link_operators_single_dependency():
    dag = DAG("test_dag", default_args={"start_date": start_date}, schedule_interval="@once")
    activity_list = [
        {"name": "test_1", "_id": 23, "dependsOn": [25]},
        {"name": "test_2", "_id": 25}
    ]
    tasks = [
        DummyOperator(task_id="sink", dag=dag),
        DummyOperator(task_id="source", dag=dag)
    ]
    link_operators(activity_list, tasks)
    assert "source" in tasks[0]._upstream_task_ids


def test_link_operators_multiple_output():
    dag = DAG("test_dag", default_args={"start_date": start_date}, schedule_interval="@once")
    activity_list = [
        {"name": "test_1", "_id": 23, "dependsOn": [25]},
        {"name": "test_2", "_id": 24, "dependsOn": [25]},
        {"name": "test_3", "_id": 25}
    ]
    tasks = [
        DummyOperator(task_id="sink_1", dag=dag),
        DummyOperator(task_id="sink_2", dag=dag),
        DummyOperator(task_id="source", dag=dag)
    ]
    link_operators(activity_list, tasks)
    assert "source" in tasks[0]._upstream_task_ids
    assert "source" in tasks[1]._upstream_task_ids


def test_link_operators_multiple_input():
    dag = DAG("test_dag", default_args={"start_date": start_date}, schedule_interval="@once")
    activity_list = [
        {"name": "test_1", "_id": 23, "dependsOn": [25, 24]},
        {"name": "test_2", "_id": 24},
        {"name": "test_3", "_id": 25}
    ]
    tasks = [
        DummyOperator(task_id="sink", dag=dag),
        DummyOperator(task_id="source_1", dag=dag),
        DummyOperator(task_id="source_2", dag=dag)
    ]
    link_operators(activity_list, tasks)
    assert "source_1" in tasks[0]._upstream_task_ids
    assert "source_2" in tasks[0]._upstream_task_ids


def test_link_operators_multiple_output_multiple_input():
    dag = DAG("test_dag", default_args={"start_date": start_date}, schedule_interval="@once")
    activity_list = [
        {"name": "test_1", "_id": 23, "dependsOn": [25, 24]},
        {"name": "test_2", "_id": 24, "dependsOn": [26]},
        {"name": "test_3", "_id": 25, "dependsOn": [26]},
        {"name": "test_4", "_id": 26}
    ]
    tasks = [
        DummyOperator(task_id="sink", dag=dag),
        DummyOperator(task_id="transform_1", dag=dag),
        DummyOperator(task_id="transform_2", dag=dag),
        DummyOperator(task_id="source", dag=dag)
    ]
    link_operators(activity_list, tasks)
    assert "transform_2" in tasks[0]._upstream_task_ids
    assert "transform_1" in tasks[0]._upstream_task_ids
    assert "source" in tasks[1]._upstream_task_ids
    assert "source" in tasks[2]._upstream_task_ids
