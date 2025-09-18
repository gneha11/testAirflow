# ============================================================
# DAG: initial_setup_with_private_packages_fabric_safe.py
# Description: Installs private packages from requirements.txt,
#              runs initial setup, logs to Airflow live.
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import sys
import logging
from pathlib import Path

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
    """
    1️⃣ Install private packages from requirements.txt
    2️⃣ Import private packages after installation
    3️⃣ Run your setup logic with live logging in Airflow
    """
    # ---------------------------
    # Install packages from requirements.txt
    # ---------------------------
    req_file = "/opt/airflow/git/testAirflow.git/dags/requirements.txt"
    if Path(req_file).exists():
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-cache-dir", "-r", req_file])
    else:
        logging.warning(f"Requirements file not found: {req_file}")

    # ---------------------------
    # Import private packages after installation
    # ---------------------------
    try:
        from data_foundation import create_schemas, create_base_folders
        from shared_utils import (
            df_get_logger,
            df_create_log_session_id,
            df_save_log_to_lakehouse,
            df_get_postgres_connection,
            df_load_config,
        )
    except ModuleNotFoundError as e:
        logging.error(f"Private package import failed: {e}")
        raise

    # ---------------------------
    # Get DAG parameters
    # ---------------------------
    params = context["params"]
    tenant_id = params.get("tenant_id", "fea7d713-34c8-451d-9fed-b0a10080c601")
    enterprise_id = params.get("enterprise_id", "initial_setup")

    # ---------------------------
    # Logger instance
    # ---------------------------
    log_session_id = df_create_log_session_id()
    logger = df_get_logger(
        name=enterprise_id,
        level=logging.INFO,
        log_to_file=True,
        tenant_id=tenant_id,
        enterprise_id=enterprise_id,
        log_session_id=log_session_id,
    )

    # ---------------------------
    # Run setup logic (without creating SparkSession manually)
    # ---------------------------
    try:
        create_schemas(None, ["telemetry_bronze", "telemetry_silver", "telemetry_gold", "asset_master"], logger)
        create_base_folders(None, ["env", "logs"], logger)
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        df_save_log_to_lakehouse(logger)
        raise
    finally:
        df_save_log_to_lakehouse(logger)
        print("✅ Log file saved to Lakehouse")


# ---------------------------
# DAG DEFINITION
# ---------------------------
with DAG(
    dag_id="initial_setup_with_private_packages_fabric_safe",
    default_args=default_args,
    description="Install private packages from requirements.txt and run initial setup with live Airflow logs",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={  # Default values (override via `--conf`)
        "tenant_id": "fea7d713-34c8-451d-9fed-b0a10080c601",
        "enterprise_id": "initial_setup",
    },
) as dag:

    initial_setup_task = PythonOperator(
        task_id="initial_setup_task",
        python_callable=run_initial_setup,
        provide_context=True,
    )
