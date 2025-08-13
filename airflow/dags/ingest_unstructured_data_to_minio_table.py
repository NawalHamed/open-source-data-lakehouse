from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
import time
import random

# ============ Configuration ============
now = datetime.utcnow()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
timestamp = now.strftime('%Y%m%dT%H%M%S')

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

BUCKET = "lakehouse"
IMAGE_COUNT = 10
RES = (800, 400)
# =======================================

# ============ Sample Flight Schema ============
FLIGHT_ROWS = [
    {"flight_number": "WB150438", "airline_iata": "WB", "departure_airport_iata": "FSE", "arrival_airport_iata": "TFQ", "aircraft_type": "Airbus A320", "distance_km": 11199.56, "status": "departed"},
    {"flight_number": "TH470271", "airline_iata": "TH", "departure_airport_iata": "MCN", "arrival_airport_iata": "MAN", "aircraft_type": "Boeing 737", "distance_km": 803.85, "status": "cancelled"},
    {"flight_number": "WU397821", "airline_iata": "WU", "departure_airport_iata": "USW", "arrival_airport_iata": "MCN", "aircraft_type": "Boeing 737", "distance_km": 10379.71, "status": "landed"},
    {"flight_number": "TC555749", "airline_iata": "TC", "departure_airport_iata": "FYP", "arrival_airport_iata": "SMZ", "aircraft_type": "Airbus A320", "distance_km": 9736.84, "status": "departed"},
    {"flight_number": "BH542960", "airline_iata": "BH", "departure_airport_iata": "TFQ", "arrival_airport_iata": "BBR", "aircraft_type": "ATR 72", "distance_km": 9280.49, "status": "departed"},
    {"flight_number": "WU671270", "airline_iata": "WU", "departure_airport_iata": "YUY", "arrival_airport_iata": "FYP", "aircraft_type": "Bombardier CRJ-900", "distance_km": 9540.2, "status": "delayed"},
    {"flight_number": "TB493929", "airline_iata": "TB", "departure_airport_iata": "FJD", "arrival_airport_iata": "USW", "aircraft_type": "ATR 72", "distance_km": 10232.99, "status": "delayed"}
]

# ============ Image Generator ============
def draw_flight_table_image(rows, image_size=(800, 400)):
    img = Image.new('RGB', image_size, color='white')
    draw = ImageDraw.Draw(img)

    try:
        font = ImageFont.truetype("arial.ttf", 14)
    except:
        font = ImageFont.load_default()

    headers = ["Flight", "Airline", "From", "To", "Aircraft", "Distance", "Status"]
    keys = ["flight_number", "airline_iata", "departure_airport_iata", "arrival_airport_iata", "aircraft_type", "distance_km", "status"]
    col_widths = [100, 70, 70, 70, 180, 100, 100]
    x_positions = [sum(col_widths[:i]) + 10 for i in range(len(col_widths))]

    y = 20
    for i, h in enumerate(headers):
        draw.text((x_positions[i], y), h, font=font, fill='black')
    y += 25

    for row in rows[:6]:  # Max 6 rows to fit
        for i, key in enumerate(keys):
            value = row[key]
            if key == "distance_km":
                value = f"{value:.0f} km"
            draw.text((x_positions[i], y), str(value), font=font, fill='black')
        y += 25

    byte_stream = BytesIO()
    img.save(byte_stream, format='JPEG')
    byte_stream.seek(0)
    return byte_stream

def generate_and_upload_airline_images():
    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)

    for i in range(IMAGE_COUNT):
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        sample = random.sample(FLIGHT_ROWS, k=min(6, len(FLIGHT_ROWS)))
        image_bytes = draw_flight_table_image(sample, RES)

        filename = f"airline_table_image_{timestamp}_{i}.jpg"

        MINIO_CLIENT.put_object(
            BUCKET,
            f"bronze_layer/{year}/{month}/{day}/images/{filename}",
            image_bytes,
            length=image_bytes.getbuffer().nbytes,
            content_type="image/jpeg"
        )
        print(f"Uploaded: {filename}")
        time.sleep(1)

# ============ Airflow DAG ============
with DAG(
    dag_id="ingest_real_airline_table_images_to_minio",
    default_args=DEFAULT_ARGS,
    description="Generate and upload real flight table images to MinIO",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["airline", "images", "minio", "ocr"]
) as dag:

    generate_images_task = PythonOperator(
        task_id="generate_and_upload_airline_table_images",
        python_callable=generate_and_upload_airline_images
    )
