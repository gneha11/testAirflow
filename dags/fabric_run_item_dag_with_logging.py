# ------------------- IMPORTS -------------------
from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator


# ------------------- CUSTOM LOGGER -------------------
class FabricLoggingMixin(LoggingMixin):
    """Wrapper to add Airflow LoggingMixin around Fabric operator"""
    def log_and_return(self, msg: str):
        self.log.info(msg)
        return msg


# ------------------- DAG DEFINITION -------------------
with DAG(
    dag_id="fabric_run_item_dag_with_logging",
    start_date=datetime(2025, 9, 18),
    catchup=False,
    schedule_interval=None,
    tags=["fabric", "lakehouse", "setup"]
) as dag:

    logger = FabricLoggingMixin()

    # ------------------- NOTEBOOK CALL -------------------
    run_schema_and_folder_setup = MSFabricRunItemOperator(
        task_id="run_schema_and_folder_setup",
        fabric_conn_id="fabric-integration",
        workspace_id="bd883325-52e5-44d1-8742-72b4e6b3be82",  # Perceptiv AIML - UAT
        item_id="18023b08-26a5-44b5-9b8a-8cde87a2280f",  # 01_notebook_lakehouse_postgres_setup
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=True
    )

    # ------------------- LOGGING WRAP -------------------
    def log_wrapper(**context):
        logger.log_and_return("✅ Triggered Fabric Notebook: Schema + Folder Setup")

    # Attach log messages before/after run
    run_schema_and_folder_setup.add_pre_execute_callback(lambda context: logger.log_and_return("🚀 Starting Fabric job..."))
    run_schema_and_folder_setup.add_post_execute_callback(lambda context, result=None: logger.log_and_return("🎯 Fabric job completed."))

    # Task
    run_schema_and_folder_setup
