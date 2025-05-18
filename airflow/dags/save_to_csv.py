import yaml
import requests
import csv
import os
import psycopg2
import sys
CONFIG_PATH = '/Users/apple/Desktop/youtube_recommender/config/config.yaml' 

# Load config
with open(CONFIG_PATH, 'r') as file:
    print("CONFIG LOADED FROM:", os.path.abspath(CONFIG_PATH))
    config = yaml.safe_load(file)

API_KEY = config['youtube']['api_key']
CHANNEL_ID = config['youtube']['channel_id']
MAX_RESULTS = config['youtube']['max_results']

def fetch_videos(api_key, channel_id, max_results):
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": api_key,
        "channelId": channel_id,
        "part": "snippet",
        "order": "date",
        "maxResults": max_results,
        "type": "video"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get('items', [])

def save_videos_to_csv(videos, filename='youtube_videos.csv'):
    if not videos:
        print("No videos to save.")
        return
    file_exists = os.path.isfile(filename)
    
    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if not file_exists:

            writer.writerow(['video_id', 'title', 'published_at', 'url'])

        for video in videos:
            video_id = video['id'].get('videoId')
            if not video_id:
                continue  # skip invalid entries
            title = video['snippet']['title']
            published_at = video['snippet']['publishedAt']
            url = f"https://youtube.com/watch?v={video_id}"
            writer.writerow([video_id, title, published_at, url])
def save_to_videos(videos):
    if not videos:
        print("no videos to insert into postgresql")
        return 
    pg_config = config['postgres']
    conn = psycopg2.connect(
        host = pg_config['host'],
        port = pg_config['port'],
        user = pg_config['user'],
        password = pg_config['password'],
        database = pg_config['database']

    )

    cursor = conn.cursor()
    for video in videos:
        video_id = video['id'].get('videoId')
        if not video_id:
            continue
        title = video['snippet']['title']
        published_at = video['snippet']['publishedAt']
        url = f"https://youtube.com/watch?v={video_id}"
        cursor.execute(
            """
            INSERT INTO videos (video_id, title, published_at, url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (video_id) DO NOTHING
            """,
            (video_id, title, published_at,url))
    conn.commit()
    cursor.close()
    conn.close()
    print("Videos inserted into PostgreSQL database successfully.")
        
if __name__ == "__main__":
    videos = fetch_videos(API_KEY, CHANNEL_ID, MAX_RESULTS)
    print(f"Fetched {len(videos)} videos")
    save_videos_to_csv(videos)
    save_to_videos(videos)
    print("Videos have been saved to CSV and PostgreSQL.")
    print("CSV file has been created as 'youtube_videos.csv'")
