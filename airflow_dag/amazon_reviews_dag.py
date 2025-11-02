from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import subprocess

# -------------------------------
# Define default arguments
# -------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -------------------------------
# Define the DAG
# -------------------------------
dag = DAG(
    'amazon_reviews_bigdata_pipeline',
    default_args=default_args,
    description='Amazon Reviews Big Data Pipeline with HuggingFace + Visualization',
    schedule_interval='@daily',
    catchup=False,
)

# -------------------------------
# Define Python tasks
# -------------------------------

def extract_data():
    print("📥 Extracting raw Amazon reviews data...")
    os.system("python scripts/extract_data.py")

def process_data():
    print("🧹 Processing and cleaning data...")
    os.system("python scripts/process_data.py")

def analyze_sentiment():
    print("🤖 Running Hugging Face sentiment analysis...")
    os.system("python scripts/sentiment_analysis.py")

def visualize_results():
    print("📊 Generating data visualizations...")
    os.system("python scripts/visualize_results.py")

# -------------------------------
# Create Airflow tasks
# -------------------------------
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

analyze_task = PythonOperator(
    task_id='analyze_sentiment',
    python_callable=analyze_sentiment,
    dag=dag,
)

visualize_task = PythonOperator(
    task_id='visualize_results',
    python_callable=visualize_results,
    dag=dag,
)

# -------------------------------
# Define task dependencies
# -------------------------------
extract_task >> process_task >> analyze_task >> visualize_task
