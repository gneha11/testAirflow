# ============================================================
# DAG: initial_setup_with_private_packages.py
# Description: Installs private packages and runs initial setup
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


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

    # Spark Session
    spark = SparkSession.builder.appName(enterprise_id).getOrCreate()

    # Log session
    log_session_id = df_create_log_session_id()

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
    dag_id="initial_setup_with_private_packages",
    default_args=default_args,
    description="Install private packages and run initial Fabric setup",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={  # 👈 Default values (override via `--conf`)
        "tenant_id": "fea7d713-34c8-451d-9fed-b0a10080c601",
        "enterprise_id": "initial_setup",
    },
) as dag:

    # Install private wheels
    install_shared_utils = BashOperator(
        task_id="install_shared_utils",
        bash_command=(
            "pip install --force-reinstall --no-deps  "
            "/opt/airflow/git/testAirflow.git/plugins/shared_utils-1.0.1-py3-none-any.whl"
        ),
    )

    install_data_foundation = BashOperator(
        task_id="install_data_foundation",
        bash_command=(
            "pip install --force-reinstall --no-deps  "
            "/opt/airflow/git/testAirflow.git/plugins/data_foundation-1.0.0-py3-none-any.whl"
        ),
    )

    # Debug: check installed packages
    list_installed_packages = BashOperator(
        task_id="list_installed_packages",
        bash_command="pip show data-foundation || pip list | grep foundation"
    )

    # Debug: check sys.path and modules
    check_python_path = BashOperator(
        task_id="check_python_path",
        bash_command=(
            "python -c 'import sys, pkgutil; "
            "print(\"\\n[PYTHON PATH]\", sys.path, \"\\n\"); "
            "print(\"[FOUND MODULES]\", [m.name for m in pkgutil.iter_modules() if \"foundation\" in m.name])'"
        )
    )

    # Run setup logic
    initial_setup_task = PythonOperator(
        task_id="initial_setup_task",
        python_callable=run_initial_setup,
        provide_context=True,
    )

    # Task dependencies
    install_shared_utils >> install_data_foundation >> list_installed_packages >> check_python_path >> initial_setup_task
