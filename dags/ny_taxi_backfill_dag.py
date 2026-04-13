from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Базовые настройки для всех задач
default_args = {
    'owner': 'fox',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# 2. (DAG)
with DAG(
    dag_id='ny_taxi_historical_backfill',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    tags=['ny_taxi', 'etl']
) as dag:

    # 3.
    run_spark_pipeline = BashOperator(
        task_id='run_spark_container',
        bash_command='cd /opt/airflow/project && docker-compose -p ny_taxi_pipeline run --rm spark_app python src/main.py --year {{ execution_date.year }} --month {{ execution_date.strftime("%m") }}'
    )