from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import json
import csv
from faker import Faker
from minio import Minio
from io import BytesIO, StringIO

# ============ CONFIGURATION ============
NUM_RECORDS = 600
MINIO_ENDPOINT = 'minio:9000'  # Use container name for Docker networks
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'warehouse'
FILE_FORMAT = 'csv'  # Options: 'json' or 'csv'
OBJECT_NAME = f"bronze_layer/structured_raw_data/weather_data/weather_data.{FILE_FORMAT}"
# =======================================

fake = Faker()

WEATHER_CONDITIONS = [
    {"code": 296, "desc": "Light Rain", "icon": "wsymbol_0017_cloudy_with_light_rain.png"},
    {"code": 299, "desc": "Moderate Rain", "icon": "wsymbol_0018_cloudy_with_heavy_rain.png"},
    {"code": 302, "desc": "Heavy Rain", "icon": "wsymbol_0025_thunderstorms.png"},
    {"code": 113, "desc": "Clear/Sunny", "icon": "wsymbol_0001_sunny.png"},
    {"code": 116, "desc": "Partly Cloudy", "icon": "wsymbol_0002_light_clouds.png"},
    {"code": 119, "desc": "Cloudy", "icon": "wsymbol_0003_white_cloud.png"},
    {"code": 122, "desc": "Overcast", "icon": "wsymbol_0004_black_low_cloud.png"},
    {"code": 143, "desc": "Mist", "icon": "wsymbol_0006_mist.png"},
    {"code": 248, "desc": "Fog", "icon": "wsymbol_0007_fog.png"},
    {"code": 260, "desc": "Freezing Fog", "icon": "wsymbol_0013_sleet_showers.png"}
]

MOON_PHASES = ["New Moon", "Waxing Crescent", "First Quarter", "Waxing Gibbous",
               "Full Moon", "Waning Gibbous", "Last Quarter", "Waning Crescent"]

WIND_DIRECTIONS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

AIRPORT_CODES_FOR_WEATHER = ["JFK", "LAX", "ATL", "ORD", "DFW", "DEN", "SFO", "CLT", "LAS", "PHX",
                             "IAH", "MIA", "SEA", "EWR", "MSP", "BOS", "DTW", "PHL", "FLL", "LGA"]

class WeatherDataGenerator:
    def __init__(self):
        self.faker = Faker()

    def _generate_location(self):
        city = self.faker.city()
        country = self.faker.country()
        region = self.faker.state() if random.random() > 0.3 else None
        lat = round(random.uniform(-90, 90), 3)
        lon = round(random.uniform(-180, 180), 3)
        query_lat = round(lat + random.uniform(-0.1, 0.1), 6)
        query_lon = round(lon + random.uniform(-0.1, 0.1), 6)

        return {
            "name": city,
            "country": country,
            "region": region,
            "lat": str(lat),
            "lon": str(lon),
            "timezone_id": "UTC",
            "localtime": (datetime.now() + timedelta(hours=random.randint(-12, 12))).strftime("%Y-%m-%d %H:%M"),
            "localtime_epoch": int((datetime.now() + timedelta(hours=random.randint(-12, 12))).timestamp()),
            "utc_offset": f"{random.choice(range(-12, 13))}.0",
            "query_point": f"{query_lat},{query_lon}"
        }

    def _generate_current_weather(self, is_day):
        condition = random.choice(WEATHER_CONDITIONS)
        temp = random.randint(-20, 40)
        return {
            "observation_time": (datetime.now() + timedelta(minutes=random.randint(-60, 60))).strftime("%I:%M %p").lstrip("0"),
            "temperature": temp,
            "weather_code": condition["code"],
            "weather_icons": [f"https://cdn.worldweatheronline.com/images/wsymbols01_png_64/{condition['icon']}"],
            "weather_descriptions": [condition["desc"]],
            "astro": self._generate_astro_data(),
            "air_quality": self._generate_air_quality(),
            "wind_speed": random.randint(0, 50),
            "wind_degree": random.randint(0, 359),
            "wind_dir": random.choice(WIND_DIRECTIONS),
            "pressure": random.randint(950, 1050),
            "precip": round(random.uniform(0, 10), 1),
            "humidity": random.randint(0, 100),
            "cloudcover": random.randint(0, 100),
            "feelslike": temp + random.randint(-5, 5),
            "uv_index": random.randint(0, 11),
            "visibility": random.randint(0, 20),
            "is_day": "yes" if is_day else "no"
        }

    def _generate_astro_data(self):
        return {
            "sunrise": f"{random.randint(5, 7)}:{random.randint(0, 59):02d} AM",
            "sunset": f"{random.randint(5, 9)}:{random.randint(0, 59):02d} PM",
            "moonrise": f"{random.randint(6, 11)}:{random.randint(0, 59):02d} PM",
            "moonset": f"{random.randint(4, 10)}:{random.randint(0, 59):02d} AM",
            "moon_phase": random.choice(MOON_PHASES),
            "moon_illumination": random.randint(0, 100)
        }

    def _generate_air_quality(self):
        return {
            "co": str(round(random.uniform(100, 500), 2)),
            "no2": str(round(random.uniform(1, 50), 2)),
            "o3": str(random.randint(10, 200)),
            "so2": str(round(random.uniform(0.1, 5), 3)),
            "pm2_5": str(round(random.uniform(0.5, 20), 3)),
            "pm10": str(round(random.uniform(1, 30), 2)),
            "us-epa-index": str(random.randint(1, 5)),
            "gb-defra-index": str(random.randint(1, 10))
        }

    def generate_weather_data(self):
        location = self._generate_location()
        is_day = random.random() > 0.5
        return {
            "request": {
                "type": "LatLon",
                "query": f"Lat {location['query_point'].split(',')[0]} and Lon {location['query_point'].split(',')[1]}",
                "language": "en",
                "unit": random.choice(["m", "f"])
            },
            "location": location,
            "current": self._generate_current_weather(is_day),
            "metadata": {
                "fetch_time": datetime.now().isoformat(),
                "airport_iata": random.choice(AIRPORT_CODES_FOR_WEATHER),
                "location_query": location["query_point"]
            }
        }

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, '|'.join(map(str, v))))
        else:
            items.append((new_key, v))
    return dict(items)

def generate_and_upload_weather_data():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    generator = WeatherDataGenerator()

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    data = [generator.generate_weather_data() for _ in range(NUM_RECORDS)]
    if FILE_FORMAT == 'json':
        byte_data = json.dumps(data, indent=2).encode('utf-8')
        content_type = 'application/json'
    else:
        flat_data = [flatten_dict(r) for r in data]
        fieldnames = list(flat_data[0].keys())
        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flat_data)
        byte_data = buffer.getvalue().encode('utf-8')
        content_type = 'text/csv'

    client.put_object(
        MINIO_BUCKET, OBJECT_NAME, data=BytesIO(byte_data), length=len(byte_data), content_type=content_type
    )
    print(f"✅ {NUM_RECORDS} weather records uploaded to MinIO as {OBJECT_NAME}")

# ============ Airflow DAG Setup ============
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='injest_weather_structured_data_from_api_to_minio',
    default_args=default_args,
    description='Generate mock weather data and save to MinIO',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['weather', 'mock', 'minio']
) as dag:

    task_generate_weather = PythonOperator(
        task_id='generate_and_upload_weather_data',
        python_callable=generate_and_upload_weather_data
    )

