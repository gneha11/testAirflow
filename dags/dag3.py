from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 18),
    "retries": 0,
}

def test_imports():
    from shared_utils import df_get_logger
    print("✅ shared_utils imported successfully")

with DAG(
    dag_id="test_df_imports_dag",
    default_args=default_args,
    description="Install wheel and test shared_utils import",
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Install shared_utils wheel
    install_wheel = BashOperator(
        task_id="install_shared_utils",
        bash_command=(
            "pip install --force-reinstall "
            "/opt/airflow/git/testAirflow/plugins/shared_utils-1.0.1-py3-none-any.whl"
        ),
    )

    # Task 2: Try importing shared_utils
    test_imports_task = PythonOperator(
        task_id="test_imports",
        python_callable=test_imports,
    )

    install_wheel >> test_imports_task
