# ============================================================
# DAG: initial_setup_with_requirements.py
# Description: Installs packages from requirements.txt, sets Java, and runs initial setup
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import sys
import os

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

    # ---------------------------
    # 1️⃣ Set Java Environment for PySpark
    # ---------------------------
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

    # ---------------------------
    # 2️⃣ Install packages from requirements.txt
    # ---------------------------
    requirements_path = "/opt/airflow/git/testAirflow.git/dags/requirements.txt"
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-cache-dir", "-r", requirements_path])

    # ---------------------------
    # 3️⃣ Imports after installation
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
    dag_id="initial_setup_with_requirements",
    default_args=default_args,
    description="Install packages from requirements.txt, set Java, and run Fabric initial setup",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "tenant_id": "fea7d713-34c8-451d-9fed-b0a10080c601",
        "enterprise_id": "initial_setup",
    },
) as dag:

    # Run setup logic (installs packages from requirements.txt + Java fix)
    initial_setup_task = PythonOperator(
        task_id="initial_setup_task",
        python_callable=run_initial_setup,
        provide_context=True,
    )
