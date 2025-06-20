from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/dags')
from kafka_s3_consumer import consume_and_save_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_s3_consumer_etl',
    default_args=default_args,
    description='ETL pipeline for consuming Kafka and saving to S3 after producer',
    schedule_interval='@hourly',  # Chạy mỗi giờ, sau producer
    start_date=datetime(2025, 6, 16),
    catchup=False,
    tags=['youtube', 'kafka', 's3']
) as dag:
    wait_for_producer = ExternalTaskSensor(
        task_id='wait_for_producer',
        external_dag_id='youtube_kafka_etl',
        external_task_id='extract_youtube_to_kafka',  # Task ID trong DAG producer
        poke_interval=60,  # Kiểm tra mỗi 60 giây
        timeout=3600,  # Timeout sau 1 giờ
        mode='poke'
    )

    consume_task = PythonOperator(
        task_id='consume_and_save_to_s3',
        python_callable=consume_and_save_to_s3
    )

    wait_for_producer >> consume_task  # Đợi producer xong mới chạy consumer