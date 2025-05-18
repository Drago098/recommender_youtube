from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os 

sys.path.insert(0, '/Users/apple/Desktop/youtube_recommender/scripts')

from save_to_csv import save_videos_to_csv,save_to_videos, fetch_videos, API_KEY, MAX_RESULTS,CHANNEL_ID

default_args = {
    'owner': 'you',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

def fetch_and_save():
    
    print("Running DAG function in file:", os.path.abspath(__file__))
   
    videos = fetch_videos(API_KEY,CHANNEL_ID, MAX_RESULTS)
    save_videos_to_csv(videos)
    print("Done saving videos to CSV")
    save_to_videos(videos)
    print("Done saving videos to PostgreSQL")


with DAG(
    dag_id='youtube_pipeline_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_save_youtube_data',
        python_callable=fetch_and_save,
    )
