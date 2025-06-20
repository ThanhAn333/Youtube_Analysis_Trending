import googleapiclient.discovery
import json
from kafka import KafkaProducer
from datetime import datetime
import logging
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# YouTube API setup
api_service_name = "youtube"
api_version = "v3"
api_key = Variable.get("YOUTUBE_API_KEY")
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

def get_category_name(category_id):
    try:
        request = youtube.videoCategories().list(
            part="snippet",
            id=category_id,
            #regionCode=region_code  # ✅ Thêm lại regionCode
        )
        response = request.execute()
        if response['items']:
            return response['items'][0]['snippet']['title']
        return "Unknown"
    except Exception as e:
        logger.error(f"Error fetching category name for {category_id} in {region_code}: {str(e)}")
        return "Unknown"


def fetch_trending_videos(region_code="VN", max_results=50):
    try:
        logger.info(f"Fetching trending videos for region {region_code}")
        request = youtube.videos().list(
            part="snippet,statistics",
            chart="mostPopular",
            regionCode=region_code,
            maxResults=max_results
        )
        response = request.execute()
        logger.info(f"Fetched {len(response['items'])} videos for {region_code}")
        return response["items"]
    except Exception as e:
        logger.error(f"Error fetching data for {region_code}: {str(e)}")
        return []

def produce_to_kafka(region_code):
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    videos = fetch_trending_videos(region_code)
    for video in videos:
        category_id = video['snippet']['categoryId']
        category_name = get_category_name(category_id)
        video_data = {
            'id': video['id'],
            'title': video['snippet']['title'],
            'category_id': category_id,
            'category_name': category_name,
            'views': int(video['statistics'].get('viewCount', 0)),
            'likes': int(video['statistics'].get('likeCount', 0)),
            'comments': int(video['statistics'].get('commentCount', 0)),
            'region': region_code,
            'published_at': video['snippet']['publishedAt'],
            'timestamp': datetime.now().isoformat()
        }
        producer.send('youtube_trending', video_data)
        logger.info(f"Sent video {video_data['id']} to Kafka topic youtube_trending (Category: {category_name})")
    producer.flush()

def extract_youtube_to_kafka():
    regions = ["VN", "US", "KR"]
    for region in regions:
        produce_to_kafka(region)

if __name__ == "__main__":
    extract_youtube_to_kafka()