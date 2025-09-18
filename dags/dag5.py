from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 16),
    "retries": 0,
}

with DAG(
    dag_id="list_testAirflow_git",
    default_args=default_args,
    description="List everything inside /opt/airflow/git/testAirflow.git",
    schedule_interval=None,
    catchup=False,
) as dag:

    list_testairflow_git = BashOperator(
        task_id="list_testairflow_git",
        bash_command="ls -R /opt/airflow/git/testAirflow.git || true"
    )
