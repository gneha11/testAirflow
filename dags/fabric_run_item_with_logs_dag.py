from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from fabric_with_logs_operator import MSFabricRunItemWithLiveLogsOperator

with DAG(
    dag_id="fabric_run_item_with_logs_dag",
    start_date=days_ago(1),
    catchup=False,
    schedule_interval=None,
) as dag:

    run_with_logs = MSFabricRunItemWithLiveLogsOperator(
        task_id="run_schema_and_folder_setup",
        fabric_conn_id="fabric-integration",
        workspace_id="bd883325-52e5-44d1-8742-72b4e6b3be82",
        item_id="18023b08-26a5-44b5-9b8a-8cde87a2280f",
        job_type="RunNotebook",
        deferrable=True,        # live log streaming
        poll_interval=10,
    )
