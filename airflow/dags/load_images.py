from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
import random
import string
import time

# ============ Configuration ============
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

MINIO_CLIENT = Minio(
    "minio:9009",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

BUCKET = "warehouse"
IMAGE_COUNT = 10  # Number of images with airline tables per run
# =======================================


# ============ Helper to Generate Random Airline Data ============
def generate_airline_data():
    airlines = ["Oman Air", "Qatar Airways", "Emirates", "Lufthansa", "Air India"]
    countries = ["Oman", "Qatar", "UAE", "Germany", "India"]

    data = []
    for _ in range(10):
        flight_no = f"{random.choice(string.ascii_uppercase)}{random.randint(100, 999)}"
        airline = random.choice(airlines)
        country = random.choice(countries)
        seats = random.randint(50, 300)
        data.append({
            "Flight No": flight_no,
            "Airline": airline,
            "Country": country,
            "Seats": seats
        })

    return pd.DataFrame(data)


# ============ Image Generator with Table ============
def generate_and_upload_airline_images():
    """Generates table images of airline data and uploads to MinIO"""

    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)

    for i in range(IMAGE_COUNT):
        df = generate_airline_data()
        
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.axis('tight')
        ax.axis('off')
        table = ax.table(cellText=df.values, colLabels=df.columns, loc='center')
        table.scale(1, 1.5)

        image_buffer = BytesIO()
        plt.savefig(image_buffer, format='jpg', bbox_inches='tight')
        image_buffer.seek(0)
        plt.close(fig)

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        filename = f"airline_data_table_{timestamp}_{i}.jpg"

        MINIO_CLIENT.put_object(
            BUCKET,
            f"bronze_layer/unstructured_images_raw_data/{filename}",
            image_buffer,
            length=image_buffer.getbuffer().nbytes,
            content_type="image/jpeg"
        )
        print(f"✅ Uploaded airline table image: {filename}")

        time.sleep(1)  # Optional delay


# ============ Airflow DAG ============
with DAG(
    dag_id="generate_airline_table_images_to_minio",
    default_args=DEFAULT_ARGS,
    description="Generate airline data tables as images and upload to MinIO",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["airline", "minio", "images"]
) as dag:

    generate_airline_images_task = PythonOperator(
        task_id="generate_and_upload_airline_table_images",
        python_callable=generate_and_upload_airline_images
    )
