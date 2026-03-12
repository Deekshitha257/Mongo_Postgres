from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.pipeline import run_pipeline, run_enquiry_pipeline


default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False
}


with DAG(
    dag_id="mongo_to_postgres_pipeline1",
    start_date=datetime(2026, 3, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["mongo", "postgres", "etl"]
) as dag:

    orders_pipeline = PythonOperator(
        task_id="orders_pipeline",
        python_callable=run_pipeline
    )

    enquiry_pipeline = PythonOperator(
        task_id="enquiry_pipeline",
        python_callable=run_enquiry_pipeline
    )

    orders_pipeline >> enquiry_pipeline