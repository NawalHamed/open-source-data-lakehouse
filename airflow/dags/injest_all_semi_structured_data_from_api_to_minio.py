from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import json
import csv
from io import BytesIO, StringIO
from string import ascii_uppercase
from itertools import product
from minio import Minio

# =================== CONFIGURATION ===================
NUM_AIRLINE_RECORDS = 500
MAX_AIRLINE_RECORDS = 500000
NUM_AIRPORT_RECORDS = 50
MAX_AIRPORT_RECORDS = 500
FILE_FORMAT = 'json'  # Options: 'json' or 'csv'
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_SECURE = False

MINIO_BUCKET = 'warehouse'  # Shared bucket for both datasets

# Correct Object Names
OBJECT_NAME_AIRLINE = f"bronze_layer/semi_structured_raw_data/airline_data/airlines_data.{FILE_FORMAT}"
OBJECT_NAME_AIRPORT = f"bronze_layer/semi_structured_raw_data/airport_data/airports_data.{FILE_FORMAT}"
# ====================================================

# ---------- Airline Data Configuration ----------
PREDEFINED_AIRLINES = [
    {"name": "American Airlines", "iata": "AA", "icao": "AAL", "callsign": "AMERICAN"},
    {"name": "Delta Air Lines", "iata": "DL", "icao": "DAL", "callsign": "DELTA"},
    {"name": "United Airlines", "iata": "UA", "icao": "UAL", "callsign": "UNITED"}
]

COUNTRIES = {
    "US": {"name": "United States", "hubs": ["ATL", "DFW", "ORD", "LAX", "JFK"]},
    "DE": {"name": "Germany", "hubs": ["FRA", "MUC", "TXL"]},
    "FR": {"name": "France", "hubs": ["CDG", "ORY", "LYS"]},
    "JP": {"name": "Japan", "hubs": ["NRT", "HND"]},
    "GB": {"name": "United Kingdom", "hubs": ["LHR", "LGW", "MAN"]},
    "IN": {"name": "India", "hubs": ["DEL", "BOM", "BLR"]},
}
AIRLINE_TYPES = ["scheduled", "charter", "cargo", "low-cost"]

# ---------- Airline Generator ----------
class AirlineGenerator:
    def __init__(self):
        self.used_iata = set()
        self.used_icao = set()
        self._init_code_pools()

    def _init_code_pools(self):
        for a in PREDEFINED_AIRLINES:
            self.used_iata.add(a["iata"])
            self.used_icao.add(a["icao"])
        self.available_iata = [f"{a}{b}" for a, b in product(ascii_uppercase, repeat=2) if f"{a}{b}" not in self.used_iata]
        self.available_icao = [f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3) if f"{a}{b}{c}" not in self.used_icao]
        random.shuffle(self.available_iata)
        random.shuffle(self.available_icao)

    def _get_codes(self, index):
        if index < len(PREDEFINED_AIRLINES):
            return PREDEFINED_AIRLINES[index]["iata"], PREDEFINED_AIRLINES[index]["icao"]
        iata = self.available_iata.pop()
        icao = self.available_icao.pop()
        self.used_iata.add(iata)
        self.used_icao.add(icao)
        return iata, icao

    def generate_airline(self, index):
        iata, icao = self._get_codes(index)
        if index < len(PREDEFINED_AIRLINES):
            name = PREDEFINED_AIRLINES[index]["name"]
            callsign = PREDEFINED_AIRLINES[index]["callsign"]
        else:
            country = random.choice(list(COUNTRIES.values()))["name"]
            name = f"{country.split()[0]} {random.choice(['Air', 'Airways', 'Airlines'])}"
            callsign = name.replace(" ", "").upper()[:8]

        country_code = random.choice(list(COUNTRIES.keys()))
        country = COUNTRIES[country_code]
        hub = random.choice(country["hubs"])
        founded = random.randint(1920, datetime.now().year - 1)
        airline_age = datetime.now().year - founded
        fleet_size = max(5, int(random.gauss(300 if founded < 1980 else 100, 200 if founded < 1980 else 80)))
        avg_age = max(1, min(30, round(airline_age / random.uniform(8, 12), 1)))

        return {
            "id": str(1000000 + index),
            "name": name,
            "iata": iata,
            "icao": icao,
            "callsign": callsign,
            "country_code": country_code,
            "country": country["name"],
            "hub": hub,
            "founded": founded,
            "fleet_size": fleet_size,
            "fleet_age": avg_age,
            "status": "active" if random.random() < 0.95 else "inactive",
            "type": random.choice(AIRLINE_TYPES),
            "timestamp": datetime.utcnow().isoformat()
        }

# ---------- Airport Generator ----------
class AirportGenerator:
    def __init__(self):
        self.used_iata = set()
        self.used_icao = set()
        self.used_ids = set()
        self.available_iata = [f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3)]
        self.available_icao = [f"{a}{b}{c}{d}" for a, b, c, d in product(ascii_uppercase, repeat=4)]
        random.shuffle(self.available_iata)
        random.shuffle(self.available_icao)

    def _get_codes(self):
        iata = self.available_iata.pop()
        icao = self.available_icao.pop()
        self.used_iata.add(iata)
        self.used_icao.add(icao)
        return iata, icao

    def _get_id(self):
        new_id = str(random.randint(3000000, 4000000))
        while new_id in self.used_ids:
            new_id = str(random.randint(3000000, 4000000))
        self.used_ids.add(new_id)
        return new_id

    def generate_airport(self, index):
        iata, icao = self._get_codes()
        country_code = random.choice(list(COUNTRIES.keys()))
        country = COUNTRIES[country_code]
        name = f"{country['name'].split()[0]} {random.choice(['International', 'Regional', 'Municipal', 'Airfield', 'Airport'])}"

        return {
            "id": self._get_id(),
            "airport_id": str(index + 1),
            "iata_code": iata,
            "icao_code": icao,
            "country_iso2": country_code,
            "country_name": country["name"],
            "latitude": str(round(random.uniform(-90, 90), 5)),
            "longitude": str(round(random.uniform(-180, 180), 5)),
            "airport_name": name,
            "timezone": "UTC"
        }

# ---------- Helpers ----------
def load_existing_data(client, object_name):
    try:
        obj = client.get_object(MINIO_BUCKET, object_name)
        content = obj.read()
        return json.loads(content) if FILE_FORMAT == 'json' else list(csv.DictReader(content.decode().splitlines()))
    except:
        return []

# ---------- Airline Task ----------
def generate_airline_data():
    generator = AirlineGenerator()
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE)
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    data = [generator.generate_airline(i) for i in range(NUM_AIRLINE_RECORDS)]
    existing = load_existing_data(client, OBJECT_NAME_AIRLINE)
    full_data = (existing + data)[-MAX_AIRLINE_RECORDS:]
    byte_data = json.dumps(full_data, indent=2).encode('utf-8')
    client.put_object(MINIO_BUCKET, OBJECT_NAME_AIRLINE, BytesIO(byte_data), len(byte_data), "application/json")
    print(f"✅ Uploaded {len(full_data)} airline records to {OBJECT_NAME_AIRLINE}")

# ---------- Airport Task ----------
def generate_airport_data():
    generator = AirportGenerator()
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE)
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    data = [generator.generate_airport(i) for i in range(NUM_AIRPORT_RECORDS)]
    existing = load_existing_data(client, OBJECT_NAME_AIRPORT)
    full_data = (existing + data)[-MAX_AIRPORT_RECORDS:]
    byte_data = json.dumps(full_data, indent=2).encode('utf-8')
    client.put_object(MINIO_BUCKET, OBJECT_NAME_AIRPORT, BytesIO(byte_data), len(byte_data), "application/json")
    print(f"✅ Uploaded {len(full_data)} airport records to {OBJECT_NAME_AIRPORT}")

# ========== Airflow DAG ==========
default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}

with DAG(
    dag_id='injest_airline_and_airport_data_to_minio',
    default_args=default_args,
    description='Generate Airline & Airport Data and Upload to MinIO (bronze layer)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data-generation', 'minio', 'airlines', 'airports']
) as dag:

    airline_task = PythonOperator(
        task_id='generate_airline_data',
        python_callable=generate_airline_data
    )

    airport_task = PythonOperator(
        task_id='generate_airport_data',
        python_callable=generate_airport_data
    )

    airline_task >> airport_task  # Run airport task after airlines

