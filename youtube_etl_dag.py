from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Thêm đường dẫn đến thư mục chứa youtube_data_ingestion.py
sys.path.append('/opt/airflow/dags')
from youtube_data_ingestion import extract_youtube_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'youtube_trending_etl',
    default_args=default_args,
    description='ETL pipeline for YouTube trending data',
    schedule_interval='@hourly',  # Chạy hàng ngày, có thể đổi thành '@hourly'
    start_date=datetime(2025, 6, 15),
    catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id='extract_youtube_data',
        python_callable=extract_youtube_data
    )