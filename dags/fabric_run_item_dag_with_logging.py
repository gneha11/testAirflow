from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator

with DAG(
    dag_id="fabric_run_item_dag_1",
    start_date=datetime(2025, 9, 18),
    catchup=False,
    schedule_interval=None
) as dag:

    # Notebook
    run_item = MSFabricRunItemOperator(
        task_id="runNotebookTask1",
        fabric_conn_id="fabric-integration",
        workspace_id="bd883325-52e5-44d1-8742-72b4e6b3be82",  # Perceptiv AIML - UAT
        item_id="18023b08-26a5-44b5-9b8a-8cde87a2280f",  # 01_notebook_lakehouse_postgres_setup
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=True
    )

    # Task will run when DAG is triggered
    run_item
