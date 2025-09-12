from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from io import BytesIO
from minio import Minio
import time

now = datetime.utcnow()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
timestamp = now.strftime('%Y%m%dT%H%M%S')

# ============ Configuration ============
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

MINIO_CLIENT = Minio(
    "minio:9009",
    access_key="minioadmin",
    secret_key="123hhbj211hjb1464",
    secure=False
)

BUCKET = "lakehouse"
IMAGE_COUNT = 10  # Number of random images to generate per run
RES = (800, 400)  # Image resolution
# =======================================


# ============ Image Generator ============

def generate_and_upload_airline_images():
    """Simulates generating table images by fetching from a dummy API and uploading to MinIO"""

    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)

    for i in range(IMAGE_COUNT):
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

        # Example placeholder API simulating airline table images
        image_url = f"https://dummyimage.com/{RES[0]}x{RES[1]}/000/fff.jpg&text=Airline+Table+{i+1}"

        response = requests.get(image_url)

        if 'image' not in response.headers.get("Content-Type", ""):
            print(f"Failed to download image for table {i+1}")
            continue

        image_data = BytesIO(response.content)
        filename = f"airline_table_image_{timestamp}_{i}.jpg"

        MINIO_CLIENT.put_object(
            BUCKET,
            f"bronze_layer/{year}/{month}/{day}/images/{filename}",
            image_data,
            length=image_data.getbuffer().nbytes,
            content_type="image/jpeg"
        )
        print(f"Uploaded airline table image: {filename}")

        time.sleep(1)


# ============ Airflow DAG ============

with DAG(
    dag_id="ingest_airline_table_images_unstructured_to_minio",
    default_args=DEFAULT_ARGS,
    description="Fetch simulated airline table images and upload to MinIO without PIL/matplotlib",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["airline", "minio", "images"]
) as dag:

    generate_images_task = PythonOperator(
        task_id="generate_and_upload_airline_table_images",
        python_callable=generate_and_upload_airline_images
    )

