from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import json
from io import BytesIO
from string import ascii_uppercase
from itertools import product
from minio import Minio

# ============== CONFIGURATION =================

MINIO_ENDPOINT = 'minio:9009'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'lakehouse'
MASTER_AIRLINE_PATH = "bronze_layer/master/airlines_data.json"
MASTER_AIRPORT_PATH = "bronze_layer/master/airports_data.json"

COUNTRIES = {
    "US": {"name": "United States", "hubs": ["ATL", "DFW", "ORD", "LAX", "JFK"]},
    "DE": {"name": "Germany", "hubs": ["FRA", "MUC", "TXL"]},
    "FR": {"name": "France", "hubs": ["CDG", "ORY", "LYS"]},
    "JP": {"name": "Japan", "hubs": ["NRT", "HND"]},
    "GB": {"name": "United Kingdom", "hubs": ["LHR", "LGW", "MAN"]},
    "IN": {"name": "India", "hubs": ["DEL", "BOM", "BLR"]},
}

# ============== DATA GENERATORS ==============

class AirlineGenerator:
    def __init__(self):
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

# ============== HELPERS ==============

def load_existing_data(client, object_name):
    try:
        obj = client.get_object(MINIO_BUCKET, object_name)
        return json.loads(obj.read())
    except:
        return []

# ============== TASKS ==============

def add_new_airline():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_data(client, MASTER_AIRLINE_PATH)

    now = datetime.utcnow().isoformat()
    ag = AirlineGenerator()

    # Ensure no duplicate IATA codes
    existing_iatas = {a["iata"] for a in data}
    ag.available_iata = [code for code in ag.available_iata if code not in existing_iatas]
    random.shuffle(ag.available_iata)

    new_airline = ag.generate_airline(len(data), now)
    data.append(new_airline)

    client.put_object(MINIO_BUCKET, MASTER_AIRLINE_PATH, BytesIO(json.dumps(data).encode()), len(json.dumps(data)), "application/json")
    print(f"✅ Added new airline: {new_airline['name']} ({new_airline['iata']})")

def add_new_airport():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_data(client, MASTER_AIRPORT_PATH)

    now = datetime.utcnow().isoformat()
    ag = AirportGenerator()

    # Ensure no duplicate IATA codes
    existing_iatas = {a["iata_code"] for a in data}
    ag.available_iata = [code for code in ag.available_iata if code not in existing_iatas]
    random.shuffle(ag.available_iata)

    new_airport = ag.generate_airport(len(data), now)
    data.append(new_airport)

    client.put_object(MINIO_BUCKET, MASTER_AIRPORT_PATH, BytesIO(json.dumps(data).encode()), len(json.dumps(data)), "application/json")
    print(f"✅ Added new airport: {new_airport['airport_name']} ({new_airport['iata_code']})")

# ============== DAG CONFIGURATION ==============

default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}

with DAG(
    dag_id='update_master_data',
    default_args=default_args,
    schedule_interval=None,  # Only run manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['minio', 'master-data', 'additions']
) as dag:

    t1 = PythonOperator(task_id='add_airline', python_callable=add_new_airline)
    t2 = PythonOperator(task_id='add_airport', python_callable=add_new_airport)

    t1 >> t2
