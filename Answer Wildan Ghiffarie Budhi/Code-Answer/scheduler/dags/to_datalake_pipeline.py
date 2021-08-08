from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskMarker

from extract.extract import extract_data_from_git

default_args = {
    'owner': 'airflow',
    'retries': 10,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    "to_datalake_pipeline",
    start_date=datetime(2021, 8, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args
) as dag:

    # ========================================= Sensors =========================================

    dag_end = ExternalTaskMarker(
        task_id="to_datalake_pipeline_end",
        external_dag_id="standardized_pipeline",
        external_task_id="standardized_pipeline_start",
    )
    
    # ========================================= Extract =========================================

    extract_from_git = PythonOperator(
        task_id="extract_from_git",
        python_callable= extract_data_from_git
    ) 

    extract_from_git >> dag_end