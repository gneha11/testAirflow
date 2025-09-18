# ============================================================
# DAG: initial_setup_with_private_packages.py
# Description: Installs private packages, PySpark, and runs initial setup
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import sys

# ---------------------------
# Default Arguments
# ---------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


# ---------------------------
# PYTHON TASK: INITIAL SETUP
# ---------------------------
def run_initial_setup(**context):
    import logging
    import subprocess

    # ---------------------------
    # Install required packages at runtime
    # ---------------------------
    packages_to_install = [
        "/opt/airflow/git/testAirflow.git/plugins/shared_utils-1.0.1-py3-none-any.whl",
        "/opt/airflow/git/testAirflow.git/plugins/data_foundation-1.0.0-py3-none-any.whl",
        "pyspark==4.0.1",
    ]

    for pkg in packages_to_install:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-cache-dir", pkg])

    # ---------------------------
    # Imports after installation
    # ---------------------------
    from pyspark.sql import SparkSession
    from data_foundation import create_schemas, create_base_folders
    from shared_utils import (
        df_get_logger,
        df_create_log_session_id,
        df_save_log_to_lakehouse,
        df_get_postgres_connection,
        df_load_config,
    )

    # Get parameters from DAG run
    params = context["params"]
    tenant_id = params.get("tenant_id", "fea7d713-34c8-451d-9fed-b0a10080c601")
    enterprise_id = params.get("enterprise_id", "initial_setup")

    # Log session
    log_session_id = df_create_log_session_id()

    # Start Spark session
    spark = SparkSession.builder.appName("data_foundation").getOrCreate()

    # Logger instance
    logger_instance = df_get_logger(
        name=enterprise_id,
        level=logging.INFO,
        log_to_file=True,
        tenant_id=tenant_id,
        enterprise_id=enterprise_id,
        log_session_id=log_session_id,
    )

    # Create schemas
    try:
        create_schemas(
            spark,
            ["telemetry_bronze", "telemetry_silver", "telemetry_gold", "asset_master"],
            logger_instance,
        )
    except Exception as e:
        logger_instance.error(f"❌ Failed to create schemas: {e}")
        df_save_log_to_lakehouse(logger_instance)
        raise
    else:
        try:
            create_base_folders(spark, ["env", "logs"], logger_instance)
        except Exception as e:
            logger_instance.error(f"❌ Failed to create base folders: {e}")
            df_save_log_to_lakehouse(logger_instance)
            raise
        finally:
            df_save_log_to_lakehouse(logger_instance)
            print("✅ Log file saved to Lakehouse")


# ---------------------------
# DAG DEFINITION
# ---------------------------
with DAG(
    dag_id="initial_setup_with_private_packages_2",
    default_args=default_args,
    description="Install private packages, PySpark, and run initial Fabric setup",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={  # Default values (override via `--conf`)
        "tenant_id": "fea7d713-34c8-451d-9fed-b0a10080c601",
        "enterprise_id": "initial_setup",
    },
) as dag:

    # Run setup logic (with inline package installation)
    initial_setup_task = PythonOperator(
        task_id="initial_setup_task",
        python_callable=run_initial_setup,
        provide_context=True,
    )
