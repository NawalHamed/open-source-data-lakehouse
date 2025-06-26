from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import requests
import time
import random
import json
from io import BytesIO
from minio import Minio

# ========== Configuration =============
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

MINIO_CLIENT = Minio("minio:9009", access_key="minioadmin", secret_key="minioadmin", secure=False)
BUCKET = "satellite-images-no-pil"
if not MINIO_CLIENT.bucket_exists(BUCKET):
    MINIO_CLIENT.make_bucket(BUCKET)

GIBS_LAYER = "VIIRS_SNPP_CorrectedReflectance_TrueColor"
BBOX = "-180,-90,180,90"
RES = (2048, 1024)

CITIES = [
    {"name": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"name": "Tokyo", "country": "JP", "lat": 35.6895, "lon": 139.6917},
    {"name": "London", "country": "UK", "lat": 51.5074, "lon": -0.1278},
    {"name": "Muscat", "country": "OM", "lat": 23.5859, "lon": 58.4059},
]

# ========== Core Processing =============
def fetch_weather(city):
    """Generates random mock weather data."""
    temp_c = round(random.uniform(-10, 40), 1)
    cloud_pct = random.randint(0, 100)
    return {"temp_c": temp_c, "cloud_pct": cloud_pct}

def process_image_no_overlay():
    """Download satellite image and save raw image + weather metadata to MinIO."""
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
        "REQUEST": "GetSnapshot",
        "BBOX": BBOX,
        "CRS": "EPSG:4326",
        "LAYERS": GIBS_LAYER,
        "WRAP": "day",
        "FORMAT": "image/jpeg",
        "WIDTH": RES[0],
        "HEIGHT": RES[1],
        "TIME": date_str
    }

    response = requests.get("https://wvs.earthdata.nasa.gov/api/v1/snapshot", params=params)
    if 'image' not in response.headers.get("Content-Type", ""):
        raise Exception("Failed to fetch image")

    img_bytes = BytesIO(response.content)
    img_filename = f"{GIBS_LAYER.replace('/', '_')}_{timestamp}.jpg"

    MINIO_CLIENT.put_object(
        BUCKET,
        f"images/{img_filename}",
        img_bytes,
        length=len(response.content),
        content_type="image/jpeg"
    )
    print(f"✅ Uploaded image: {img_filename}")

    # Weather metadata
    weather_data = []
    for city in CITIES:
        weather = fetch_weather(city)
        city_record = {**city, **weather}
        weather_data.append(city_record)

    metadata = {
        "timestamp": datetime.utcnow().isoformat(),
        "weather_overlay": weather_data,
        "layer": GIBS_LAYER,
        "bbox": BBOX,
        "resolution": f"{RES[0]}x{RES[1]}",
    }

    meta_filename = f"{GIBS_LAYER.replace('/', '_')}_{timestamp}_metadata.json"
    meta_bytes = BytesIO(json.dumps(metadata, indent=2).encode())

    MINIO_CLIENT.put_object(
        BUCKET,
        f"metadata/{meta_filename}",
        meta_bytes,
        length=meta_bytes.getbuffer().nbytes,
        content_type="application/json"
    )
    print(f"✅ Uploaded metadata: {meta_filename}")

# ========== Airflow DAG =============
with DAG(
    dag_id="satellite_processing_without_pil",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["satellite", "weather", "no-pil"]
) as dag:

    start_task = DummyOperator(task_id="start")

    process_task = PythonOperator(
        task_id="download_and_save_satellite_image",
        python_callable=process_image_no_overlay
    )

    end_task = DummyOperator(task_id="end")

    start_task >> process_task >> end_task
