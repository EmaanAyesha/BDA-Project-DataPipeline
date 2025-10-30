
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.ingest_data import ingest_data
from scripts.process_data import process_data
from scripts.sentiment_analysis import run_sentiment_analysis
from scripts.load_to_mongodb import load_to_mongodb
from scripts.visualize_results import visualize_results

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025,10,30),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('amazon_reviews_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    t1 = PythonOperator(task_id='ingest', python_callable=ingest_data)
    t2 = PythonOperator(task_id='process', python_callable=process_data)
    t3 = PythonOperator(task_id='sentiment', python_callable=run_sentiment_analysis)
    t4 = PythonOperator(task_id='load_mongo', python_callable=load_to_mongodb)
    t5 = PythonOperator(task_id='visualize', python_callable=visualize_results)
    t1 >> t2 >> t3 >> t4 >> t5
