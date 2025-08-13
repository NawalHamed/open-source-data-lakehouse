from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import json
from io import BytesIO
from string import ascii_uppercase
from itertools import product
from minio import Minio


# =================== CONFIGURATION ===================
# Number of records to generate for each dataset
NUM_AIRLINE_RECORDS = 50
NUM_AIRPORT_RECORDS = 10
NUM_FLIGHT_RECORDS = 500

# MinIO connection details
MINIO_ENDPOINT = 'minio:9009'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'lakehouse'
FILE_FORMAT = 'json'

# File paths for master datasets
MASTER_AIRLINE_PATH = "bronze_layer/master/airlines_data.json"
MASTER_AIRPORT_PATH = "bronze_layer/master/airports_data.json"

# Country codes, names, and airport hubs
COUNTRIES = {
    "US": {"name": "United States", "hubs": ["ATL", "DFW", "ORD", "LAX", "JFK"]},
    "DE": {"name": "Germany", "hubs": ["FRA", "MUC", "TXL"]},
    "FR": {"name": "France", "hubs": ["CDG", "ORY", "LYS"]},
    "JP": {"name": "Japan", "hubs": ["NRT", "HND"]},
    "GB": {"name": "United Kingdom", "hubs": ["LHR", "LGW", "MAN"]},
    "IN": {"name": "India", "hubs": ["DEL", "BOM", "BLR"]},
}

# List of possible aircraft types
AIRCRAFT_TYPES = ["Boeing 737", "Boeing 747", "Airbus A320", "Airbus A380", "Embraer E190", "Bombardier CRJ-900", "ATR 72"]

# =================== DATA GENERATORS ===================
class AirlineGenerator:
    def __init__(self):
        # Pre-generate all possible codes and shuffle them
        self.available_iata = [f"{a}{b}" for a, b in product(ascii_uppercase, repeat=2)]
        self.available_icao = [f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3)]
        random.shuffle(self.available_iata)
        random.shuffle(self.available_icao)

    def generate_airline(self, index, timestamp):
        iata = self.available_iata.pop()
        icao = self.available_icao.pop()
        country_code = random.choice(list(COUNTRIES.keys()))
        country = COUNTRIES[country_code]
        name = f"{country['name'].split()[0]} {random.choice(['Air', 'Airways', 'Airlines'])}"
        callsign = name.replace(" ", "").upper()[:8]
        
        return {
            "id": str(1000000 + index),
            "name": name,
            "iata": iata,
            "icao": icao,
            "callsign": callsign,
            "country_code": country_code,
            "country": country['name'],
            "hub": random.choice(country['hubs']),
            "status": "active",
            "created_at": timestamp,
            "updated_at": timestamp
        }

class AirportGenerator:
    def __init__(self):
        self.available_iata = [f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3)]
        self.available_icao = [f"{a}{b}{c}{d}" for a, b, c, d in product(ascii_uppercase, repeat=4)]
        random.shuffle(self.available_iata)
        random.shuffle(self.available_icao)

    def generate_airport(self, index, timestamp):
        iata = self.available_iata.pop()
        icao = self.available_icao.pop()
        country_code = random.choice(list(COUNTRIES.keys()))
        country = COUNTRIES[country_code]
        name = f"{country['name'].split()[0]} {random.choice(['International', 'Regional', 'Airport'])}"
        
        return {
            "id": str(2000000 + index),
            "airport_id": str(index + 1),
            "iata_code": iata,
            "icao_code": icao,
            "country_iso2": country_code,
            "country_name": country['name'],
            "airport_name": name,
            "created_at": timestamp,
            "updated_at": timestamp
        }

class FlightGenerator:
    def __init__(self, airline_iatas, airport_iatas):
        self.airline_iatas = airline_iatas
        self.airport_iatas = airport_iatas

    def generate_flight(self, timestamp):
        airline = random.choice(self.airline_iatas)
        dep = random.choice(self.airport_iatas)
        arr = random.choice([a for a in self.airport_iatas if a != dep] or [dep])

        return {
            "flight_id": f"FL-{random.randint(100000,999999)}",
            "flight_number": f"{airline}{random.randint(100,999999)}",
            "airline_iata": airline,
            "departure_airport_iata": dep,
            "arrival_airport_iata": arr,
            "aircraft_type": random.choice(AIRCRAFT_TYPES),
            "distance_km": round(random.uniform(500, 12000), 2),
            "status": random.choice(["scheduled", "departed", "landed", "delayed", "cancelled"]),
            "created_at": timestamp,
            "updated_at": timestamp
        }

# =================== HELPERS ===================
def load_existing_data(client, object_name):
    try:
        obj = client.get_object(MINIO_BUCKET, object_name)
        return json.loads(obj.read())
    except Exception:
        return []

def object_exists(client, object_name):
    try:
        client.stat_object(MINIO_BUCKET, object_name)
        return True
    except:
        return False

# =================== TASKS ===================
def generate_airline_master():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if object_exists(client, MASTER_AIRLINE_PATH):
        print("Airline master already exists, skipping.")
        return
    
    now = datetime.utcnow().isoformat()
    ag = AirlineGenerator()
    data = [ag.generate_airline(i, now) for i in range(NUM_AIRLINE_RECORDS)]
    
    client.put_object(MINIO_BUCKET, MASTER_AIRLINE_PATH, BytesIO(json.dumps(data).encode()), len(json.dumps(data)), "application/json")
    print(f"Master Airline data generated: {len(data)} records")

def generate_airport_master():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if object_exists(client, MASTER_AIRPORT_PATH):
        print("Airport master already exists, skipping.")
        return
    
    now = datetime.utcnow().isoformat()
    ag = AirportGenerator()
    data = [ag.generate_airport(i, now) for i in range(NUM_AIRPORT_RECORDS)]
    
    client.put_object(MINIO_BUCKET, MASTER_AIRPORT_PATH, BytesIO(json.dumps(data).encode()), len(json.dumps(data)), "application/json")
    print(f"Master Airport data generated: {len(data)} records")

def generate_flight_data():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    
    airline_data = load_existing_data(client, MASTER_AIRLINE_PATH)
    airport_data = load_existing_data(client, MASTER_AIRPORT_PATH)

    airline_iatas = [a['iata'] for a in airline_data]
    airport_iatas = [a['iata_code'] for a in airport_data]

    now = datetime.utcnow()
    timestamp = now.isoformat()
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    object_name = f"bronze_layer/{year}/{month}/{day}/json/flight_data/flight_data_{now.strftime('%H%M%S')}.json"

    fg = FlightGenerator(airline_iatas, airport_iatas)
    data = [fg.generate_flight(timestamp) for _ in range(NUM_FLIGHT_RECORDS)]

    client.put_object(MINIO_BUCKET, object_name, BytesIO(json.dumps(data).encode()), len(json.dumps(data)), "application/json")
    print(f"Flight data generated: {len(data)} records to {object_name}")

# =================== DAG CONFIGURATION ===================
default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}

with DAG(
    dag_id='injest_all_semi_structured_data_from_api_to_minio',
    default_args=default_args,
    schedule_interval='@daily', # Run every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['minio', 'master-data', 'flights']
) as dag:
    # Task 1: Generate airline master data
    t1 = PythonOperator(task_id='generate_airline_master', python_callable=generate_airline_master)
    # Task 2: Generate airport master data
    t2 = PythonOperator(task_id='generate_airport_master', python_callable=generate_airport_master)
    # Task 3: Generate daily flight data
    t3 = PythonOperator(task_id='generate_flight_data', python_callable=generate_flight_data)
    
    # Task dependencies: airline -> airport -> flights
    t1 >> t2 >> t3
