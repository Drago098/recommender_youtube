from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os 
import subprocess 

sys.path.insert(0, '/Users/apple/Desktop/youtube_recommender/scripts')

from save_to_csv import save_videos_to_csv,save_to_videos, fetch_videos, API_KEY, MAX_RESULTS,CHANNEL_ID

default_args = {
    'owner': 'you',
    'start_date': datetime(2024, 1, 1),
    'email' : ['hack.ai26081998@gmail.com'],
    'email_on_failure': True,
    'email_on_success': True,
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

    #====DVC + GIT AUTOMATION====
    try:
        subprocess.run(["dvc","add", "youtube_videos.csv"], check = True)
        subprocess.run(["git", "add", "youtube_videos.csv.dvc"], check = True)
        subprocess.run(["git", "commit", "-m", f"Daily update: {datetime.now().date()}"], check=True)
        subprocess.run (["dvc", "push"], check = True)
        print("Successfully pushed changes to DVC and Git.")
    except:
        print("Failed to push changes to DVC and Git.")
def send_status_email():
    import smtplib

    sender = "hack.ai26081998@gmail.com"
    receiver = "hack.ai26081998@gmail.com"
    app_password = "yunukhyfedspznoq"  # Replace with actual app password

    message = """\
Subject: YouTube DAG Run Successful

The DAG has completed successfully and the data has been synced."""

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender, app_password)
        server.sendmail(sender, receiver, message)




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
    send_email_task = PythonOperator(
        task_id='send_status_email',
        python_callable=send_status_email,
    )

    fetch_task >> send_email_task

