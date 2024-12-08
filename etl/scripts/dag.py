from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.etl import run_etl

# Define default arguments for the DAG
default_args = {
    "start_date": datetime(2024, 10, 1),
    "catchup": False,
}

# Create the DAG
with DAG(
    dag_id="big5_etl",
    default_args=default_args,
    schedule_interval="@daily",
    description="ETL process for the Big 5 stocks",
) as dag:

    # Define the ETL task
    etl_task = PythonOperator(
        task_id="run_big5_etl",
        python_callable=run_etl,
    )

    # Set the task in the DAG
    etl_task
    