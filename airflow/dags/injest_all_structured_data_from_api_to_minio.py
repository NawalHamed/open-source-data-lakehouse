from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import json
import csv
from io import BytesIO, StringIO
from minio import Minio
from itertools import product
from string import ascii_uppercase

# ============ CONFIGURATION ============
NUM_WEATHER = 500
NUM_COUNTRIES = 50
NUM_CITIES = 200

MINIO_ENDPOINT = 'minio:9009'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'lakehouse'
FILE_FORMAT = 'csv'

#timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%S')

now = datetime.utcnow()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
timestamp = now.strftime('%Y%m%dT%H%M%S')

OBJECT_NAME_WEATHER = f"bronze_layer/{year}/{month}/{day}/csv/weather_data/weather_data_{timestamp}.{FILE_FORMAT}"
OBJECT_NAME_COUNTRIES = f"bronze_layer/{year}/{month}/{day}/csv/countries_data/countries_data_{timestamp}.{FILE_FORMAT}"
OBJECT_NAME_CITIES = f"bronze_layer/{year}/{month}/{day}/csv/cities_data/cities_data_{timestamp}.{FILE_FORMAT}"
# =======================================

# ---------- Weather Data ----------
CITIES = ["New York", "London", "Berlin", "Tokyo", "Muscat", "Paris", "Toronto", "Dubai"]
COUNTRIES = ["USA", "UK", "Germany", "Japan", "Oman", "France", "Canada", "UAE"]
REGIONS = ["East", "West", "North", "South", None]

WEATHER_CONDITIONS = [
    {"code": 296, "desc": "Light Rain"},
    {"code": 299, "desc": "Moderate Rain"},
    {"code": 302, "desc": "Heavy Rain"},
    {"code": 113, "desc": "Clear/Sunny"},
    {"code": 116, "desc": "Partly Cloudy"},
    {"code": 119, "desc": "Cloudy"},
]

def generate_weather_record():
    city = random.choice(CITIES)
    country = random.choice(COUNTRIES)
    return {
        "city": city,
        "country": country,
        "region": random.choice(REGIONS),
        "temperature_c": random.randint(-10, 45),
        "humidity": random.randint(20, 100),
        "wind_speed_kmh": random.randint(0, 100),
        "weather_desc": random.choice(WEATHER_CONDITIONS)["desc"],
        "timestamp": datetime.utcnow().isoformat(),
         "created_at": timestamp, "updated_at": timestamp
    }

# ---------- Countries Data ----------
PREDEFINED_COUNTRIES = [
    {"name": "United States", "iso2": "US", "capital": "Washington", "continent": "NA", "population": 331000000},
    {"name": "Germany", "iso2": "DE", "capital": "Berlin", "continent": "EU", "population": 83000000},
    {"name": "Japan", "iso2": "JP", "capital": "Tokyo", "continent": "AS", "population": 125000000},
    {"name": "France", "iso2": "FR", "capital": "Paris", "continent": "EU", "population": 67000000},
]

def generate_country(index):
    if index < len(PREDEFINED_COUNTRIES):
        c = PREDEFINED_COUNTRIES[index].copy()  # Copy to avoid mutating the original
    else:
        name = f"Country{index}"
        c = {
            "name": name,
            "iso2": name[:2].upper(),
            "capital": f"{name}City",
            "continent": random.choice(["AF", "EU", "AS", "NA"]),
            "population": random.randint(100000, 50000000)
        }
    c["id"] = str(1000000 + index)
    c["created_at"] = timestamp
    c["updated_at"] = timestamp
    return c

# ---------- Cities Data ----------
COUNTRY_CODES = ["US", "DE", "JP", "FR", "OM", "CA", "GB"]

class CityGenerator:
    def __init__(self):
        self.used_iata = set()
        self.iata_pool = [f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3)]
        random.shuffle(self.iata_pool)

    def generate_city(self, index):
        iata = self.iata_pool.pop()
        return {
            "id": str(2000000 + index),
            "iata_code": iata,
            "city_name": f"City{index}",
            "country_iso2": random.choice(COUNTRY_CODES),
            "latitude": round(random.uniform(-90, 90), 6),
            "longitude": round(random.uniform(-180, 180), 6),
            "timezone": "UTC",
             "created_at": timestamp, "updated_at": timestamp
        }

# ---------- Helpers ----------
def flatten_dict(d):
    return {k: '|'.join(v) if isinstance(v, list) else v for k, v in d.items()}

def upload_to_minio(data, object_name):
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    if FILE_FORMAT == 'json':
        byte_data = json.dumps(data, indent=2).encode()
        ctype = 'application/json'
    else:
        flat_data = [flatten_dict(r) for r in data]
        buf = StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(flat_data[0].keys()))
        writer.writeheader()
        writer.writerows(flat_data)
        byte_data = buf.getvalue().encode()
        ctype = 'text/csv'

    client.put_object(MINIO_BUCKET, object_name, data=BytesIO(byte_data), length=len(byte_data), content_type=ctype)
    print(f"✅ Uploaded {len(data)} records to {object_name}")

# ---------- Airflow Tasks ----------
def generate_weather():
    data = [generate_weather_record() for _ in range(NUM_WEATHER)]
    upload_to_minio(data, OBJECT_NAME_WEATHER)

def generate_countries_task():
    data = [generate_country(i) for i in range(NUM_COUNTRIES)]
    upload_to_minio(data, OBJECT_NAME_COUNTRIES)

def generate_cities_task():
    g = CityGenerator()
    data = [g.generate_city(i) for i in range(NUM_CITIES)]
    upload_to_minio(data, OBJECT_NAME_CITIES)

# ---------- Airflow DAG ----------
default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=2)}

with DAG(
    dag_id='ingest_all_structured_data_from_api_to_minio',
    default_args=default_args,
    description='Generate Weather, Countries, Cities Data and upload to MinIO',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['structured-data', 'minio', 'weather', 'countries', 'cities']
) as dag:

    t1 = PythonOperator(task_id='generate_weather_data', python_callable=generate_weather)
    t2 = PythonOperator(task_id='generate_countries_data', python_callable=generate_countries_task)
    t3 = PythonOperator(task_id='generate_cities_data', python_callable=generate_cities_task)

    t1 >> t2 >> t3  # Weather first, then countries, then cities

