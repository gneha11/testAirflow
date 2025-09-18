from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 16),
    "retries": 0,
}

def test_imports():
    from shared_utils import df_get_logger
    print("shared_utils imported successfully")

with DAG(
    dag_id="test_df_imports_dag",
    default_args=default_args,
    description="Test if shared_utils can be imported",
    schedule_interval=None,
    catchup=False,
) as dag:

    test_imports_task = PythonOperator(
        task_id="test_imports",
        python_callable=test_imports,
    )

