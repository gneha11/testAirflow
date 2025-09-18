# ============================================================
# DAG: fabric_notebook_full_setup.py
# Description: Installs private packages, PySpark, Fabric SDK,
#              and runs a Fabric notebook with live log streaming
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import subprocess
import sys
import logging

# ==============================
# Default arguments
# ==============================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# ==============================
# Python callable: install + run notebook
# ==============================
def install_and_run_notebook(**context):
    logger = LoggingMixin().log

    # -----------------------------
    # Step 1: Install packages
    # -----------------------------
    packages_to_install = [
        "/opt/airflow/git/testAirflow.git/plugins/shared_utils-1.0.1-py3-none-any.whl",
        "/opt/airflow/git/testAirflow.git/plugins/data_foundation-1.0.0-py3-none-any.whl",
        "pyspark==4.0.1",
        "azure-fabric-sdk"  # Fabric SDK
    ]

    for pkg in packages_to_install:
        logger.info(f"Installing package: {pkg}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-cache-dir", pkg])
    logger.info("✅ All packages installed successfully")

    # -----------------------------
    # Step 2: Import modules after installation
    # -----------------------------
    from pyspark.sql import SparkSession
    from data_foundation import create_schemas, create_base_folders
    from shared_utils import (
        df_get_logger,
        df_create_log_session_id,
        df_save_log_to_lakehouse,
        df_get_postgres_connection,
        df_load_config,
    )
    from azure.fabric import NotebookClient  # Fabric SDK

    # -----------------------------
    # Step 3: Initialize parameters
    # -----------------------------
    params = context["params"]
    tenant_id = params.get("tenant_id", "fea7d713-34c8-451d-9fed-b0a10080c601")
    enterprise_id = params.get("enterprise_id", "initial_setup")

    # ✅ Your Fabric details
    workspace_id = "bd883325-52e5-44d1-8742-72b4e6b3be82"  # Perceptiv AIML - UAT
    item_id = "18023b08-26a5-44b5-9b8a-8cde87a2280f"       # 01_notebook_lakehouse_postgres_setup

    workspace_url = "https://api.fabric.microsoft.com"  # Base API URL for Fabric

    log_session_id = df_create_log_session_id()

    # -----------------------------
    # Step 4: Start Spark session
    # -----------------------------
    spark = SparkSession.builder.appName("data_foundation").getOrCreate()

    # -----------------------------
    # Step 5: Logger instance
    # -----------------------------
    logger_instance = df_get_logger(
        name=enterprise_id,
        level=logging.INFO,
        log_to_file=True,
        tenant_id=tenant_id,
        enterprise_id=enterprise_id,
        log_session_id=log_session_id,
    )

    # -----------------------------
    # Step 6: Create schemas + base folders
    # -----------------------------
    try:
        create_schemas(
            spark,
            ["telemetry_bronze", "telemetry_silver", "telemetry_gold", "asset_master"],
            logger_instance,
        )
        create_base_folders(spark, ["env", "logs"], logger_instance)
    except Exception as e:
        logger_instance.error(f"❌ Initial setup failed: {e}")
        df_save_log_to_lakehouse(logger_instance)
        raise
    finally:
        df_save_log_to_lakehouse(logger_instance)
        logger.info("✅ Initial schemas and folders setup completed")

    # -----------------------------
    # Step 7: Run Fabric notebook with live logs
    # -----------------------------
    client = NotebookClient(workspace_url)
    logger.info(f"Starting Fabric notebook in workspace {workspace_id}, item {item_id}")

    for output_line in client.run_notebook(
        workspace_id=workspace_id,
        item_id=item_id,
        parameters=params,
        stream_output=True  # Live log streaming
    ):
        logger.info(output_line)

    logger.info(f"✅ Fabric notebook {item_id} completed successfully")


# ==============================
# DAG Definition
# ==============================
with DAG(
    dag_id="fabric_notebook_full_setup",
    default_args=default_args,
    description="Install private packages + PySpark + Fabric SDK + run Fabric notebook with live logs",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "tenant_id": "fea7d713-34c8-451d-9fed-b0a10080c601",
        "enterprise_id": "initial_setup",
    },
) as dag:

    # Run installation + notebook in single PythonOperator
    run_full_setup = PythonOperator(
        task_id="install_and_run_notebook",
        python_callable=install_and_run_notebook,
        provide_context=True,
    )
