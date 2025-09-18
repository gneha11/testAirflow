from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator
from datetime import datetime

# Initialize logger
logger = LoggingMixin().log

def log_start(context):
    logger.info("🚀 Starting Fabric job...")

def log_success(context):
    logger.info("✅ Fabric job completed successfully!")

def log_failure(context):
    logger.error("❌ Fabric job failed!")

with DAG(
    dag_id="fabric_run_item_dag_with_logging",
    start_date=datetime(2025, 9, 18),
    catchup=False,
    schedule_interval=None,
    tags=["fabric", "logging"]
) as dag:

    run_schema_and_folder_setup = MSFabricRunItemOperator(
        task_id="run_schema_and_folder_setup",
        fabric_conn_id="fabric-integration",  # must exist in Airflow Connections
        workspace_id="bd883325-52e5-44d1-8742-72b4e6b3be82",  # Perceptiv AIML - UAT
        item_id="18023b08-26a5-44b5-9b8a-8cde87a2280f",       # 01_notebook_lakehouse_postgres_setup
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=True,
        on_execute_callback=log_start,
        on_success_callback=log_success,
        on_failure_callback=log_failure,
    )

    run_schema_and_folder_setup
