from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
from minio.error import S3Error
import requests
import json
import io
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Configuration (directly in code)
CONFIG = {
    'API_KEY': 'a16798cda2025004aa3e84a44b77bbe2',
    'BASE_URL': 'http://api.aviationstack.com/v1',
    'MAX_LIMIT': 5,
    'MINIO_ENDPOINT': 'minio_v:9009',  # Use 'localhost:9000' if running outside Docker
    'MINIO_ACCESS_KEY': 'minioadmin',
    'MINIO_SECRET_KEY': 'minioadmin',
    'MINIO_BUCKET': 'airline-info',
    'MINIO_SECURE': False
}

def ensure_minio_bucket():
    """Ensure MinIO bucket exists"""
    minio_client = Minio(
        CONFIG['MINIO_ENDPOINT'],
        access_key=CONFIG['MINIO_ACCESS_KEY'],
        secret_key=CONFIG['MINIO_SECRET_KEY'],
        secure=CONFIG['MINIO_SECURE']
    )
    
    if not minio_client.bucket_exists(CONFIG['MINIO_BUCKET']):
        minio_client.make_bucket(CONFIG['MINIO_BUCKET'])
        print(f"Created bucket: {CONFIG['MINIO_BUCKET']}")
    else:
        print(f"Bucket exists: {CONFIG['MINIO_BUCKET']}")

def upload_batch_to_minio(batch_data, batch_number):
    """Upload batch data to MinIO"""
    minio_client = Minio(
        CONFIG['MINIO_ENDPOINT'],
        access_key=CONFIG['MINIO_ACCESS_KEY'],
        secret_key=CONFIG['MINIO_SECRET_KEY'],
        secure=CONFIG['MINIO_SECURE']
    )
    
    object_name = f"batch/airline_batch_{batch_number:03d}.json"
    json_bytes = json.dumps(batch_data, indent=2, ensure_ascii=False).encode('utf-8')

    try:
        minio_client.put_object(
            bucket_name=CONFIG['MINIO_BUCKET'],
            object_name=object_name,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type='application/json'
        )
        print(f"Uploaded batch {batch_number} to MinIO as {object_name}")
    except S3Error as e:
        print(f"MinIO upload failed: {e}")
        raise

def fetch_airline_data(**kwargs):
    """Fetch airline data from API and upload to MinIO"""
    params = {
        'access_key': CONFIG['API_KEY'],
        'limit': CONFIG['MAX_LIMIT']
    }

    offset = 0
    batch_number = 0

    while True:
        params['offset'] = offset
        try:
            response = requests.get(f"{CONFIG['BASE_URL']}/airlines", params=params)
            response.raise_for_status()
            data = response.json()

            print(f"Offset {offset}, Batch {batch_number}")

            if 'error' in data:
                print("API Error:", data['error'].get('message', 'Unknown error'))
                break

            batch = data.get('data', [])
            if not batch:
                print("No data returned. Done.")
                break

            upload_batch_to_minio(batch, batch_number)

            if len(batch) < CONFIG['MAX_LIMIT']:
                print("All data retrieved.")
                break

            offset += CONFIG['MAX_LIMIT']
            batch_number += 1
            time.sleep(1)  # Respect API rate limits

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            raise

with DAG(
    'fetch_semi_structured_airline_data_from_api_to_minio',
    default_args=default_args,
    description='Fetches airline data from AviationStack and stores in MinIO',
    schedule_interval='@daily',
    catchup=False,
    tags=['aviation', 'data-ingestion'],
) as dag:

    ensure_bucket = PythonOperator(
        task_id='ensure_minio_bucket',
        python_callable=ensure_minio_bucket
    )

    fetch_data = PythonOperator(
        task_id='fetch_airline_data',
        python_callable=fetch_airline_data
    )

    ensure_bucket >> fetch_data
