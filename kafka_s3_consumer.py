from kafka import KafkaConsumer
import boto3
import json
from datetime import datetime
import logging

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Consumer setup
def safe_deserializer(x):
    try:
        decoded = x.decode('utf-8')
        return json.loads(decoded)
    except Exception as e:
        logger.warning(f"Lỗi khi giải mã message: {x}, lỗi: {e}")
        return None  # bỏ qua message lỗi

consumer = KafkaConsumer(
    'youtube_trending',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    value_deserializer=safe_deserializer,
    consumer_timeout_ms=30000
)

# LocalStack S3 setup
s3_client = boto3.client(
    's3',
    endpoint_url='http://localstack:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

def consume_and_save_to_s3():
    batch = []
    for message in consumer:
        video_data = message.value
        if video_data is None:
            continue
        logger.info(f"Đã nhận message: {video_data}")
        batch.append(video_data)

    if batch:  # Sau khi đọc hết mới lưu
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"raw/youtube_trending_batch_{timestamp}.json"
        try:
            s3_client.put_object(
                Bucket='youtube-trend-data',
                Key=s3_key,
                Body="\n".join(json.dumps(record) for record in batch)
            )
            logger.info(f" Đã lưu toàn bộ {len(batch)} bản ghi vào s3://youtube-trend-data/{s3_key}")
        except Exception as e:
            logger.error(f" Ghi vào S3 thất bại: {e}")

if __name__ == "__main__":
    consume_and_save_to_s3()
