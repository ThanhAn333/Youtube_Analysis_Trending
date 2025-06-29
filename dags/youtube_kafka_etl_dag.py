from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/dags')
from youtube_kafka_producer import extract_youtube_to_kafka

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'youtube_kafka_etl',
    default_args=default_args,
    description='Streaming ETL pipeline for YouTube trending data with Kafka',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 15),
    catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id='extract_youtube_to_kafka',
        python_callable=extract_youtube_to_kafka
    )
    