from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from io import BytesIO
from minio import Minio
import time

# ============ Configuration ============
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Create a MinIO client to connect to object storage
MINIO_CLIENT = Minio(
    "minio:9009",    # Use container name for Docker networks
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False # Using HTTP, not HTTPS
)

BUCKET = "warehouse" # MinIO bucket name where images will be stored
IMAGE_COUNT = 10  # Number of random images to generate per run

# NASA GIBS (Global Imagery Browse Services) settings
GIBS_LAYER = "VIIRS_SNPP_CorrectedReflectance_TrueColor"
# Bounding box for the whole Earth
BBOX = "-180,-90,180,90"
# Resolution of the image (width x height)
RES = (2048, 1024)

# ============ Image Generator ============
def generate_and_upload_images():
    """Generates multiple satellite images from NASA GIBS and uploads to MinIO"""
    
    # Create bucket if it doesn't exist
    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)

     # Loop over number of images to generate
    for i in range(IMAGE_COUNT):
        
        # Offset days to get images from previous dates
        random_day_offset = i  # or use random.randint(1, 100) for different days
        target_date = (datetime.utcnow() - timedelta(days=random_day_offset)).strftime("%Y-%m-%d")
        
        # Timestamp for unique filenames
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        
        # API parameters for NASA GIBS
        params = {
            "REQUEST": "GetSnapshot",
            "BBOX": BBOX,
            "CRS": "EPSG:4326",
            "LAYERS": GIBS_LAYER,
            "WRAP": "day",
            "FORMAT": "image/jpeg",
            "WIDTH": RES[0],
            "HEIGHT": RES[1],
            "TIME": target_date
        }

        # Send GET request to NASA API
        response = requests.get("https://wvs.earthdata.nasa.gov/api/v1/snapshot", params=params)
        
        # If response is not an image, skip to next iteration
        if 'image' not in response.headers.get("Content-Type", ""):
            print(f"Failed to download image for {target_date}")
            continue

        # Store image in memory
        image_data = BytesIO(response.content)

        # Create filename
        filename = f"{GIBS_LAYER.replace('/', '_')}_{timestamp}_{i}.jpg"

        # Upload image to MinIO under bronze layer path
        MINIO_CLIENT.put_object(
            BUCKET,
            f"bronze_layer/unstructured_images_raw_data/{filename}",
            image_data,
            length=image_data.getbuffer().nbytes,
            content_type="image/jpeg"
        )
        print(f"Uploaded image: {filename}")

        # Sleep for 1 second to avoid overloading NASA's API
        time.sleep(1)  # Small delay to avoid hammering the API


# ============ Airflow DAG ============
with DAG(
    dag_id="injest_images_unstructured_data_from_api_to_minio",
    default_args=DEFAULT_ARGS,
    description="Generate multiple satellite images and upload to MinIO without PIL",
    schedule_interval="@daily", # Run every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["satellite", "minio", "images"]
) as dag:

    generate_images_task = PythonOperator(
        task_id="generate_and_upload_satellite_images",
        python_callable=generate_and_upload_images
    )
