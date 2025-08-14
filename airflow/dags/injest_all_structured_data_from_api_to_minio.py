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

# =================== CONFIGURATION ===================

# Number of records to generate for each dataset
NUM_WEATHER_RECORDS = 250000
NUM_COUNTRY_RECORDS = 50
NUM_CITY_RECORDS = 200

# MinIO storage configuration
MINIO_ENDPOINT = 'minio:9009'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'lakehouse'
FILE_FORMAT = 'csv' # Output file format for stored data

# Paths to store master datasets in MinIO
MASTER_COUNTRY_PATH = "bronze_layer/master/countries_data.csv"
MASTER_CITY_PATH = "bronze_layer/master/cities_data.csv" 

# Sample values for weather generation
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

# Predefined country data for master dataset
PREDEFINED_COUNTRIES = [
    {"name": "United States", "iso2": "US", "capital": "Washington", "continent": "NA", "population": 331000000},
    {"name": "Germany", "iso2": "DE", "capital": "Berlin", "continent": "EU", "population": 83000000},
    {"name": "Japan", "iso2": "JP", "capital": "Tokyo", "continent": "AS", "population": 125000000},
    {"name": "France", "iso2": "FR", "capital": "Paris", "continent": "EU", "population": 67000000},
]

# ISO2 country codes for city generation
COUNTRY_CODES = ["US", "DE", "JP", "FR", "OM", "CA", "GB"]

# =================== DATA GENERATORS ===================
class WeatherGenerator:
    def generate_weather_record(self, timestamp):
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
            "timestamp": timestamp,
            "created_at": timestamp,
            "updated_at": timestamp
        }

class CountryGenerator:
    def generate_country(self, index, timestamp):
        if index < len(PREDEFINED_COUNTRIES):
            c = PREDEFINED_COUNTRIES[index].copy()
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

class CityGenerator:
    def __init__(self):
        self.used_iata = set()
        self.iata_pool = [f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3)]
        random.shuffle(self.iata_pool)

    def generate_city(self, index, timestamp):
        iata = self.iata_pool.pop()
        return {
            "id": str(2000000 + index),
            "iata_code": iata,
            "city_name": f"City{index}",
            "country_iso2": random.choice(COUNTRY_CODES),
            "latitude": round(random.uniform(-90, 90), 6),
            "longitude": round(random.uniform(-180, 180), 6),
            "timezone": "UTC",
            "created_at": timestamp,
            "updated_at": timestamp
        }

# =================== HELPERS ===================
def load_existing_data(client, object_name):
    try:
        obj = client.get_object(MINIO_BUCKET, object_name)
        if object_name.endswith('.json'):
            return json.loads(obj.read())
        else:
            return list(csv.DictReader(StringIO(obj.read().decode('utf-8'))))
    except Exception:
        return []

def object_exists(client, object_name):
    try:
        client.stat_object(MINIO_BUCKET, object_name)
        return True
    except:
        return False

def upload_to_minio(data, object_name, file_format):
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    
    if file_format == 'json':
        byte_data = json.dumps(data).encode()
        content_type = 'application/json'
    else:
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        byte_data = output.getvalue().encode()
        content_type = 'text/csv'
    
    client.put_object(
        MINIO_BUCKET,
        object_name,
        BytesIO(byte_data),
        len(byte_data),
        content_type=content_type
    )

# =================== TASKS ===================
def generate_country_master():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if object_exists(client, MASTER_COUNTRY_PATH):
        print("Country master already exists, skipping.")
        return
    
    now = datetime.utcnow().isoformat()
    cg = CountryGenerator()
    data = [cg.generate_country(i, now) for i in range(NUM_COUNTRY_RECORDS)]
    
    upload_to_minio(data, MASTER_COUNTRY_PATH, FILE_FORMAT)
    print(f"Master Country data generated: {len(data)} records")

def generate_city_master():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if object_exists(client, MASTER_CITY_PATH):
        print("City master already exists, skipping.")
        return
    
    now = datetime.utcnow().isoformat()
    cg = CityGenerator()
    data = [cg.generate_city(i, now) for i in range(NUM_CITY_RECORDS)]
    
    upload_to_minio(data, MASTER_CITY_PATH, FILE_FORMAT)
    print(f"Master City data generated: {len(data)} records")

def generate_weather_data():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    
    now = datetime.utcnow()
    timestamp = now.isoformat()
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    object_name = f"bronze_layer/{year}/{month}/{day}/csv/weather_data/weather_data_{now.strftime('%H%M%S')}.csv"

    wg = WeatherGenerator()
    data = [wg.generate_weather_record(timestamp) for _ in range(NUM_WEATHER_RECORDS)]

    upload_to_minio(data, object_name, FILE_FORMAT)
    print(f"Weather data generated: {len(data)} records to {object_name}")

# =================== DAG CONFIGURATION ===================

# Default task arguments for Airflow DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
with DAG(
    dag_id='injest_all_structured_data_from_api_to_minio',
    default_args=default_args,
    schedule_interval='@daily', # Run every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['minio', 'master-data', 'weather', 'countries', 'cities']
) as dag:
    
 # Task definitions
    #t1 = PythonOperator(task_id='generate_country_master', python_callable=generate_country_master)
    #t2 = PythonOperator(task_id='generate_city_master', python_callable=generate_city_master)
    t3 = PythonOperator(task_id='generate_weather_data', python_callable=generate_weather_data)
    
 # Task dependencies: countries → cities → weather
    t3
