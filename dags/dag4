from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="list_directories_dag",
    default_args=default_args,
    schedule_interval=None,  # run manually
    catchup=False,
    tags=["debug"],
) as dag:

    list_dirs = BashOperator(
        task_id="list_all_dirs",
        bash_command="ls -R /opt/airflow/git/ || true"
    )

    list_plugins = BashOperator(
        task_id="list_plugins_dir",
        bash_command="ls -R /opt/airflow/plugins/ || true"
    )

    list_dags = BashOperator(
        task_id="list_dags_dir",
        bash_command="ls -R /opt/airflow/dags/ || true"
    )

    list_dirs >> list_plugins >> list_dags
