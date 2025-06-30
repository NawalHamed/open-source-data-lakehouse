from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
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


# ============ Generate Table Image with Pillow ============
def dataframe_to_image(df):
    rows = df.shape[0]
    cols = df.shape[1]
    cell_height = 30
    cell_width = 150

    img_height = (rows + 1) * cell_height + 20
    img_width = cols * cell_width + 20

    image = Image.new("RGB", (img_width, img_height), "white")
    draw = ImageDraw.Draw(image)

    # Optional: Use default font or load a TTF font if available
    try:
        font = ImageFont.truetype("arial.ttf", 14)
    except:
        font = ImageFont.load_default()

    # Draw header
    for idx, col in enumerate(df.columns):
        draw.text((idx * cell_width + 10, 10), str(col), fill="black", font=font)

    # Draw rows
    for row in range(rows):
        for col in range(cols):
            text = str(df.iloc[row, col])
            x = col * cell_width + 10
            y = (row + 1) * cell_height + 10
            draw.text((x, y), text, fill="black", font=font)

    return image


# ============ Image Generator and Uploader ============
def generate_and_upload_airline_images():
    """Generates table images of airline data and uploads to MinIO"""

    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)

    for i in range(IMAGE_COUNT):
        df = generate_airline_data()
        image = dataframe_to_image(df)

        image_buffer = BytesIO()
        image.save(image_buffer, format='JPEG')
        image_buffer.seek(0)

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        filename = f"airline_data_table_{timestamp}_{i}.jpg"

        MINIO_CLIENT.put_object(
            BUCKET,
            f"bronze_layer/unstructured_images_raw_data/{filename}",
            image_buffer,
            length=image_buffer.getbuffer().nbytes,
            content_type="image/jpeg"
        )

        print(f"Uploaded airline table image: {filename}")
        time.sleep(1)


# ============ Airflow DAG ============
with DAG(
    dag_id="generate_airline_table_images_to_minio",
    default_args=DEFAULT_ARGS,
    description="Generate airline data tables as images with Pillow and upload to MinIO",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["airline", "minio", "images"]
) as dag:

    generate_airline_images_task = PythonOperator(
        task_id="generate_and_upload_airline_table_images",
        python_callable=generate_and_upload_airline_images
    )
