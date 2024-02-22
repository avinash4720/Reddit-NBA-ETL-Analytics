from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
import praw
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
#nltk.download('all')

# Initialize Reddit client globally (consider using Airflow Connections for secrets)
reddit = praw.Reddit(client_id='#fill',
                     client_secret='#fill',
                     user_agent='#fill')

# AWS S3 details
s3_client = boto3.client('s3')
target_bucket_name = '#bucketname'

def categorize_sentiment(text):
    sia = SentimentIntensityAnalyzer()
    score = sia.polarity_scores(text)
    if score['compound'] > 0.05:
        return 1  # Positive
    elif score['compound'] < -0.05:
        return -1  # Negative
    else:
        return 0  # Neutral

def extract_reddit_data(**kwargs):
    subreddit_name = kwargs['subreddit']
    limit = kwargs.get('limit', 100)  # Default to 100 posts if no limit specified
    subreddit = reddit.subreddit(subreddit_name)
    top_posts = subreddit.top(limit=limit)
    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file_str = 'reddit_data_' + date_now_string
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    posts_data = [{
        "title": post.title,
        "score": post.score,
        "id": post.id,
        "url": post.url,
        "num_comments": post.num_comments,
        "created_utc": post.created_utc,
        "author": str(post.author),  # Convert author object to string
        "over_18": post.over_18,
        "edited": post.edited,
        "spoiler": post.spoiler,
        "stickied": post.stickied
    } for post in top_posts]
    output_list = [file_str, posts_data]
    return output_list

def transform_reddit_data(task_instance):
    posts_data = task_instance.xcom_pull(task_ids="extract_reddit_task")[1]
    object_key = task_instance.xcom_pull(task_ids="extract_reddit_task")[0]
    df = pd.DataFrame(posts_data)

    # Convert 'created_utc' from UNIX timestamp to datetime
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')

    # Ensure boolean fields are correctly formatted
    df['over_18'] = df['over_18'].astype(bool)
    df['edited'] = df['edited'].apply(lambda x: False if x == False else True)
    df['spoiler'] = df['spoiler'].astype(bool)
    df['stickied'] = df['stickied'].astype(bool)

    # Ensure integer fields are correctly formatted
    df['num_comments'] = df['num_comments'].astype(int)
    df['score'] = df['score'].astype(int)

    # Ensure string fields are correctly formatted
    df['title'] = df['title'].astype(str)
    df['url'] = df['url'].astype(str)
    df['author'] = df['author'].astype(str)
    df['id'] = df['id'].astype(str)

    df['sentiment'] = df['title'].apply(lambda x: categorize_sentiment(x))

    # Save the transformed data to CSV
    csv_data = df.to_csv(index=False)
    object_key = f"{object_key}.csv"
    df.to_csv(object_key, index=False)

    return [csv_data, object_key]



def load_to_s3(task_instance, **kwargs):
    csv_data = task_instance.xcom_pull(task_ids="transform_reddit_task")[0]
    file_name = task_instance.xcom_pull(task_ids="transform_reddit_task")[1]
    s3_client.put_object(Bucket=target_bucket_name, Key=file_name, Body=csv_data)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('reddit_data_analytics_dag',
         default_args=default_args,
         #schedule_interval='@weekly',
         catchup=False) as dag:

    extract_reddit_task = PythonOperator(
        task_id='extract_reddit_task',
        python_callable=extract_reddit_data,
        op_kwargs={'subreddit': '#subreddit', 'limit': 100}
    )

    transform_reddit_task = PythonOperator(
        task_id='transform_reddit_task',
        python_callable=transform_reddit_data
    )

    load_to_s3_task = PythonOperator(
        task_id='load_to_s3_task',
        python_callable=load_to_s3
    )

    extract_reddit_task >> transform_reddit_task >> load_to_s3_task