from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os
import sys
import platform

def check_spark_environment(**kwargs):
    print("=== 1️⃣ Checking Java Installation ===")
    try:
        subprocess.run(["java", "-version"], check=True)
        java_home = os.environ.get("JAVA_HOME")
        print(f"JAVA_HOME: {java_home}")
        if not java_home:
            print("⚠️ JAVA_HOME is not set.")
    except Exception as e:
        print(f"❌ Java check failed: {e}")

    print("\n=== 2️⃣ Checking PySpark & Python Version ===")
    print(f"Python version: {platform.python_version()}")
    try:
        import pyspark
        print(f"PySpark version: {pyspark.__version__}")
        if int(pyspark.__version__.split(".")[0]) < 3:
            print("⚠️ PySpark version may be incompatible with Python 3.12")
    except ImportError:
        print("❌ PySpark is not installed")

    print("\n=== 3️⃣ Checking Environment Variables ===")
    spark_home = os.environ.get("SPARK_HOME")
    print(f"SPARK_HOME: {spark_home}")
    if not spark_home:
        print("⚠️ SPARK_HOME is not set.")

    print("\n=== 4️⃣ Checking Resources ===")
    try:
        import psutil
        mem = psutil.virtual_memory()
        cpu_count = psutil.cpu_count()
        print(f"CPU cores: {cpu_count}")
        print(f"Total memory: {mem.total / (1024**3):.2f} GB")
    except ImportError:
        print("⚠️ psutil not installed, cannot check memory/cpu info")

    print("\n✅ Environment check completed.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 18),
    'retries': 0
}

with DAG(
    'spark_env_check',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['diagnostics'],
) as dag:

    env_check_task = PythonOperator(
        task_id='check_spark_env',
        python_callable=check_spark_environment,
        provide_context=True
    )
