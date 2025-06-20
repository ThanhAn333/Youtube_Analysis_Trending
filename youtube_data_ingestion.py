import googleapiclient.discovery
import boto3
import json
from datetime import datetime

# YouTube API setup
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyARq-Now4RkanuxlQqLq9rqUNEkZ2dzQgc"  # Thay bằng API key thực tế
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# LocalStack S3 setup
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)
def fetch_trending_videos(region_code="VN", max_results=50):
    try:
        request = youtube.videos().list(
            part="snippet,statistics",
            chart="mostPopular",
            regionCode=region_code,
            maxResults=max_results
        )
        response = request.execute()
        return response["items"]
    except Exception as e:
        print(f"Error fetching data for {region_code}: {str(e)}")
        return []
def save_to_s3(data, region_code):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"raw/youtube_trending_{region_code}_{timestamp}.json"
    
    s3_client.put_object(
        Bucket='youtube-trend-data',
        Key=s3_key,
        Body=json.dumps(data, ensure_ascii=False, indent=2)
    )
    print(f"Saved data to s3://youtube-trend-data/{s3_key}")
def extract_youtube_data():
    regions = ["VN", "US", "KR"]
    for region in regions:
        data = fetch_trending_videos(region)
        if data:
            save_to_s3(data, region)

if __name__ == "__main__":
    extract_youtube_data()