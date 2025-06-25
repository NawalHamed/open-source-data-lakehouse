from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import requests
import json
import time
from io import BytesIO
import pytz
import math
import random
from PIL import Image, ImageDraw, ImageFont, ImageEnhance, ImageFilter
from minio import Minio
from airflow.models import Variable

# ====== Configuration =======
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# MinIO config
MINIO_CLIENT = Minio("minio:9009",
                    access_key="minioadmin",
                    secret_key="minioadmin",
                    secure=False)

BUCKET = "enhanced-satellite"
if not MINIO_CLIENT.bucket_exists(BUCKET):
    MINIO_CLIENT.make_bucket(BUCKET)

# GIBS config
GIBS_LAYER = "VIIRS_SNPP_CorrectedReflectance_TrueColor"
BBOX = "-180,-90,180,90"
RES = (4096, 2048)

# Cities with timezones
WEATHER_CITIES = [
    {"name": "New York", "country": "US", "timezone": "America/New_York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Los Angeles", "country": "US", "timezone": "America/Los_Angeles", "lat": 34.0522, "lon": -118.2437},
    {"name": "Chicago", "country": "US", "timezone": "America/Chicago", "lat": 41.8781, "lon": -87.6298},
    {"name": "Toronto", "country": "CA", "timezone": "America/Toronto", "lat": 43.65107, "lon": -79.347015},
    {"name": "London", "country": "GB", "timezone": "Europe/London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Paris", "country": "FR", "timezone": "Europe/Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "Berlin", "country": "DE", "timezone": "Europe/Berlin", "lat": 52.52, "lon": 13.405},
    {"name": "Tokyo", "country": "JP", "timezone": "Asia/Tokyo", "lat": 35.6895, "lon": 139.6917},
    {"name": "Dubai", "country": "AE", "timezone": "Asia/Dubai", "lat": 25.276987, "lon": 55.296249},
    {"name": "Sydney", "country": "AU", "timezone": "Australia/Sydney", "lat": -33.8688, "lon": 151.2093},
    {"name": "Mumbai", "country": "IN", "timezone": "Asia/Kolkata", "lat": 19.0760, "lon": 72.8777},
    {"name": "Beijing", "country": "CN", "timezone": "Asia/Shanghai", "lat": 39.9042, "lon": 116.4074},
    {"name": "Cairo", "country": "EG", "timezone": "Africa/Cairo", "lat": 30.0444, "lon": 31.2357},
    {"name": "Rio", "country": "BR", "timezone": "America/Sao_Paulo", "lat": -22.9068, "lon": -43.1729},
    {"name": "Mexico City", "country": "MX", "timezone": "America/Mexico_City", "lat": 19.4326, "lon": -99.1332}
]

# Font setup
try:
    FONT = ImageFont.truetype("arial.ttf", 28)
except Exception:
    FONT = ImageFont.load_default()

# ====== Helper Functions =======
def latlon_to_pixel(lat, lon, bbox, width, height):
    """Converts latitude/longitude to pixel coordinates on the image."""
    west, south, east, north = map(float, bbox.split(','))
    x = int((lon - west) / (east - west) * width)
    y = int((north - lat) / (north - south) * height)
    return x, y

def fetch_weather(city):
    """Generates mock weather data for a city."""
    temp_c = round(random.uniform(-10, 40), 1)       # Random temp °C
    cloud_pct = random.randint(0, 100)                # Random cloud %
    return temp_c, cloud_pct

def create_simple_icon(icon_type, size=32):
    """Creates a simple weather icon."""
    icon = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(icon)
    center = size // 2

    if icon_type == "sun":
        draw.ellipse((center - 10, center - 10, center + 10, center + 10), fill="yellow")
        for angle in range(0, 360, 45):
            x1 = center + 12 * math.cos(math.radians(angle))
            y1 = center + 12 * math.sin(math.radians(angle))
            x2 = center + 16 * math.cos(math.radians(angle))
            y2 = center + 16 * math.sin(math.radians(angle))
            draw.line((x1, y1, x2, y2), fill="yellow", width=2)
    elif icon_type == "cloud":
        draw.ellipse((8, 14, 22, 28), fill="lightgray")
        draw.ellipse((12, 10, 26, 24), fill="lightgray")
        draw.ellipse((16, 14, 30, 28), fill="lightgray")
    elif icon_type == "rain":
        draw.ellipse((8, 14, 22, 28), fill="lightgray")
        draw.ellipse((12, 10, 26, 24), fill="lightgray")
        draw.ellipse((16, 14, 30, 28), fill="lightgray")
        for drop_x in [14, 20, 26]:
            draw.line((drop_x, 28, drop_x, 32), fill="blue", width=2)
    return icon

ICONS = {
    "sun": create_simple_icon("sun"),
    "cloud": create_simple_icon("cloud"),
    "rain": create_simple_icon("rain")
}

def enforce_storage_retention(bucket_name, prefix="images/", max_size_bytes=5 * 1024**3):
    """Enforces a maximum storage size by deleting oldest files."""
    objects = list(MINIO_CLIENT.list_objects(bucket_name, prefix=prefix, recursive=True))
    objects.sort(key=lambda o: o.last_modified)
    total_size = sum(o.size for o in objects)

    if total_size <= max_size_bytes:
        return

    print(f"⚠️ Storage usage {total_size/(1024**3):.2f} GB exceeds limit {max_size_bytes/(1024**3):.2f} GB, deleting oldest files...")

    for obj in objects:
        MINIO_CLIENT.remove_object(bucket_name, obj.object_name)
        total_size -= obj.size
        print(f"Deleted {obj.object_name}, freed {obj.size/(1024**2):.2f} MB")
        if total_size <= max_size_bytes:
            break

# ====== Image Processing Function =======
def process_single_image(execution_time=None):
    """Processes a single satellite image at the given time"""
    if execution_time is None:
        execution_time = datetime.utcnow()
    
    DATE = (execution_time - timedelta(days=1)).strftime("%Y-%m-%d")
    timestamp = execution_time.strftime("%Y%m%dT%H%M%S")
    
    params = {
        "REQUEST": "GetSnapshot",
        "BBOX": BBOX,
        "CRS": "EPSG:4326",
        "LAYERS": GIBS_LAYER,
        "WRAP": "day",
        "FORMAT": "image/jpeg",
        "WIDTH": RES[0],
        "HEIGHT": RES[1],
        "TIME": DATE
    }

    response = requests.get("https://wvs.earthdata.nasa.gov/api/v1/snapshot", params=params)
    if 'image' not in response.headers.get("Content-Type", ""):
        raise ValueError("Invalid image response from API")

    image = Image.open(BytesIO(response.content))
    if image.mode != "RGBA":
        image = image.convert("RGBA")

    # Apply image enhancements
    image = image.filter(ImageFilter.SHARPEN)
    image = ImageEnhance.Contrast(image).enhance(1.1)
    image = ImageEnhance.Color(image).enhance(1.05)

    draw = ImageDraw.Draw(image)
    weather_info = []

    # Overlay weather information for predefined cities
    for city in WEATHER_CITIES:
        x, y = latlon_to_pixel(city["lat"], city["lon"], BBOX, *RES)
        temp_c, cloud_pct = fetch_weather(city)

        local_now = datetime.now(pytz.timezone(city["timezone"]))
        hour = local_now.hour
        is_night = hour < 6 or hour > 18

        if is_night:
            overlay = Image.new("RGBA", image.size)
            overlay_draw = ImageDraw.Draw(overlay)
            overlay_draw.ellipse((x - 16, y - 16, x + 16, y + 16), fill=(0, 0, 0, 120))
            image = Image.alpha_composite(image, overlay)

        draw.ellipse((x - 6, y - 6, x + 6, y + 6), fill="red")

        icon_key = None
        if cloud_pct is not None:
            if cloud_pct < 20:
                icon_key = "sun"
            elif cloud_pct < 70:
                icon_key = "cloud"
            else:
                icon_key = "rain"

        if icon_key and icon_key in ICONS:
            icon = ICONS[icon_key]
            image.paste(icon, (x - 16, y - 48), icon)

        label = f"{city['name']} | {temp_c}°C | Cloud {cloud_pct}%"
        draw.text((x + 10, y - 10), label, font=FONT, fill="white")

        weather_info.append({
            "city": city["name"],
            "country": city["country"],
            "timezone": city["timezone"],
            "lat": city["lat"],
            "lon": city["lon"],
            "temp_c": temp_c,
            "cloud_pct": cloud_pct,
            "local_hour": hour,
            "is_night": is_night
        })

    metadata = {
        "layer": GIBS_LAYER,
        "timestamp": execution_time.isoformat(),
        "date": DATE,
        "resolution": f"{RES[0]}x{RES[1]}",
        "bbox": BBOX,
        "weather_overlay": weather_info
    }

    # Embed metadata
    info = image.info
    info["Description"] = json.dumps(metadata)
    
    buffer = BytesIO()
    image.save(buffer, format="PNG", **info)
    buffer.seek(0)

    filename = f"{GIBS_LAYER.replace('/', '_')}_{timestamp}.png"
    MINIO_CLIENT.put_object(
        BUCKET,
        f"images/{filename}",
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="image/png"
    )

    print(f"✅ Uploaded: {filename}")
    
    # Enforce storage retention
    enforce_storage_retention(BUCKET, prefix="images/", max_size_bytes=5 * 1024**3)

# ====== Continuous Processing Function =======
def continuous_processing(**context):
    """Runs continuous processing similar to the original script"""
    run_duration = timedelta(hours=1)  # How long to run the processing loop
    interval = timedelta(seconds=30)   # Interval between processing
    
    start_time = datetime.utcnow()
    end_time = start_time + run_duration
    
    while datetime.utcnow() < end_time:
        try:
            current_time = datetime.utcnow()
            process_single_image(current_time)
            
            # Calculate sleep time to maintain consistent interval
            next_run = current_time + interval
            sleep_time = (next_run - datetime.utcnow()).total_seconds()
            if sleep_time > 0:
                time.sleep(sleep_time)
                
        except Exception as e:
            print(f"Error processing image: {e}")
            time.sleep(30)  # Wait before retrying

# ====== Airflow DAG Definition =======
dag = DAG(
    'satellite_image_processing_continuous',
    default_args=DEFAULT_ARGS,
    description='Continuous processing of satellite images with weather overlays',
    schedule_interval=timedelta(hours=1),  # This will start a new run every hour
    start_date=days_ago(1),
    catchup=False,
    tags=['satellite', 'imagery', 'weather', 'continuous']
)

start_task = DummyOperator(
    task_id='start_processing',
    dag=dag,
)

process_task = PythonOperator(
    task_id='ingest_raw_image_data_into_minio',
    python_callable=continuous_processing,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_processing',
    dag=dag,
)

start_task >> process_task >> end_task
