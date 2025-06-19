from datetime import datetime, timedelta
import time  # <-- Added this import
import logging
import requests
import pandas as pd
import io
from minio import Minio
from minio.error import S3Error
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
API_KEY = 'a16798cda2025004aa3e84a44b77bbe2'
BASE_URL = 'http://api.aviationstack.com/v1'
ENDPOINTS = ['cities', 'countries']
MAX_LIMIT = 5

# MinIO Configuration
MINIO_ENDPOINT = 'minio_v:9009'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_SECURE = False
MINIO_BUCKET = 'structured'  # Note: Fixed typo from 'cities-countries'

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
    # Add this to force path-style URLs
    region=None
)

def ensure_bucket():
    """Ensure the MinIO bucket exists."""
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        logging.info(f"Created bucket: {MINIO_BUCKET}")
    else:
        logging.info(f"Bucket already exists: {MINIO_BUCKET}")

def fetch_and_upload_data(endpoint, **context):
    """Fetch data from API and upload to MinIO."""
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    
    offset = 0
    batch = 0
    
    while True:
        params = {
            'access_key': API_KEY,
            'limit': MAX_LIMIT,
            'offset': offset
        }

        try:
            response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
            response.raise_for_status()
            json_data = response.json()
            records = json_data.get('data', [])

            if not records:
                logging.info(f"No more records for {endpoint}")
                break

            # Convert to CSV and upload
            df = pd.json_normalize(records)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue().encode('utf-8')
            
            object_name = f"{endpoint}/csv/{endpoint}_batch_{batch:03}.csv"
            minio_client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_name,
                data=io.BytesIO(csv_bytes),
                length=len(csv_bytes),
                content_type='text/csv'
            )
            logging.info(f"Uploaded {object_name}")

            offset += MAX_LIMIT
            batch += 1
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            logging.error(f"Error processing {endpoint}: {str(e)}")
            raise

with DAG(
    'fetch_structured_countries_data_from_api_to_minio',
    default_args=default_args,
    description='Fetch aviation data and store in MinIO',
    schedule_interval='@daily',
    catchup=False,
    tags=['data_processing'],
) as dag:

    start_task = DummyOperator(task_id='start')
    ensure_bucket_task = PythonOperator(
        task_id='ensure_bucket_exists',
        python_callable=ensure_bucket,
    )
    end_task = DummyOperator(task_id='end')

    # Create dynamic tasks for each endpoint
    fetch_tasks = []
    for endpoint in ENDPOINTS:
        task = PythonOperator(
            task_id=f'fetch_{endpoint}_data',
            python_callable=fetch_and_upload_data,
            op_kwargs={'endpoint': endpoint},
        )
        fetch_tasks.append(task)

    # Set up dependencies
    start_task >> ensure_bucket_task >> fetch_tasks >> end_task
