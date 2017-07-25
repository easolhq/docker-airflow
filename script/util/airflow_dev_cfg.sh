#!/usr/bin/bash
export PYTHONPATH=${PYTHONPATH}:$(pwd)/airflow_home
export AIRFLOW__CORE__BASE_LOG_FOLDER=$(pwd)/logs
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow_home/dags
export AIRFLOW__CORE__PLUGINS_FOLDER=$(pwd)/airflow_home/plugins
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://postgres:letmein@localhost:5432
