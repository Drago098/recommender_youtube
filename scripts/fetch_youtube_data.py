import yaml
import requests

# Load config
with open('config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

API_KEY = config['youtube']['api_key']
CHANNEL_ID = config['youtube']['channel_id']
MAX_RESULTS = config['youtube']['max_results']

def fetch_videos(api_key, channel_id):
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": api_key,
        "channelId": channel_id,
        "part": "snippet",
        "order": "date",
        "maxResults": MAX_RESULTS,
        "type": "video"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get('items', [])

if __name__ == "__main__":
    with open('config/config.yaml') as f:
        config = yaml.safe_load(f)
    videos = fetch_videos(API_KEY, CHANNEL_ID)
    print(f"Fetched {len(videos)} videos")
