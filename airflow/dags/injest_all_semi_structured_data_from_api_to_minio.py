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
NUM_AIRPORT_RECORDS = 50
NUM_FLIGHT_RECORDS = 500
MAX_RECORDS = 500000
FILE_FORMAT = 'json'
MINIO_ENDPOINT = 'minio:9009'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_SECURE = False
MINIO_BUCKET = 'lakehouse'
timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%S')

OBJECT_NAME_AIRLINE = f"bronze_layer/semi_structured_raw_data/airline_data/airlines_data_{timestamp}.{FILE_FORMAT}"
OBJECT_NAME_AIRPORT = f"bronze_layer/semi_structured_raw_data/airport_data/airports_data_{timestamp}.{FILE_FORMAT}"
OBJECT_NAME_FLIGHT = f"bronze_layer/semi_structured_raw_data/flight_data/flights_data_{timestamp}.{FILE_FORMAT}"

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
AIRCRAFT_TYPES = ["Boeing 737", "Boeing 747", "Airbus A320", "Airbus A380", "Embraer E190", "Bombardier CRJ-900", "ATR 72"]

class AirlineGenerator:
    def __init__(self):
        self.used_iata = set()
        self.used_icao = set()
        self.available_iata = [f"{a}{b}" for a, b in product(ascii_uppercase, repeat=2)]
        self.available_icao = [f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3)]
        random.shuffle(self.available_iata)
        random.shuffle(self.available_icao)

    def generate_airline(self, index):
        iata = self.available_iata.pop()
        icao = self.available_icao.pop()
        country_code = random.choice(list(COUNTRIES.keys()))
        country = COUNTRIES[country_code]
        name = f"{country['name'].split()[0]} {random.choice(['Air', 'Airways', 'Airlines'])}"
        callsign = name.replace(" ", "").upper()[:8]
        return {"id": str(1000000 + index), "name": name, "iata": iata, "icao": icao, "callsign": callsign, "country_code": country_code, "country": country['name'], "hub": random.choice(country['hubs']), "status": "active"}

class AirportGenerator:
    def __init__(self):
        self.available_iata = [f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3)]
        self.available_icao = [f"{a}{b}{c}{d}" for a, b, c, d in product(ascii_uppercase, repeat=4)]
        random.shuffle(self.available_iata)
        random.shuffle(self.available_icao)

    def generate_airport(self, index):
        iata = self.available_iata.pop()
        icao = self.available_icao.pop()
        country_code = random.choice(list(COUNTRIES.keys()))
        country = COUNTRIES[country_code]
        name = f"{country['name'].split()[0]} {random.choice(['International', 'Regional', 'Airport'])}"
        return {"id": str(2000000 + index), "airport_id": str(index + 1), "iata_code": iata, "icao_code": icao, "country_iso2": country_code, "country_name": country['name'], "airport_name": name}

class FlightGenerator:
    def __init__(self, airline_iatas, airport_iatas):
        self.airline_iatas = airline_iatas if airline_iatas else ["XX"]
        self.airport_iatas = airport_iatas if airport_iatas else ["AAA"]

    def generate_flight(self):
        airline = random.choice(self.airline_iatas)
        dep = random.choice(self.airport_iatas)
        arr = random.choice([a for a in self.airport_iatas if a != dep] or [dep])
        return {"flight_id": f"FL-{random.randint(100000,999999)}", "flight_number": f"{airline}{random.randint(100,999999)}", "airline_iata": airline, "departure_airport_iata": dep, "arrival_airport_iata": arr, "aircraft_type": random.choice(AIRCRAFT_TYPES), "distance_km": round(random.uniform(500, 12000), 2), "status": random.choice(["scheduled", "departed", "landed", "delayed", "cancelled"])}

def load_existing_data(client, object_name):
    try:
        obj = client.get_object(MINIO_BUCKET, object_name)
        return json.loads(obj.read())
    except: return []

def generate_airline_data():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(MINIO_BUCKET): client.make_bucket(MINIO_BUCKET)
    data = [AirlineGenerator().generate_airline(i) for i in range(NUM_AIRLINE_RECORDS)]
    client.put_object(MINIO_BUCKET, OBJECT_NAME_AIRLINE, BytesIO(json.dumps(data).encode()), len(json.dumps(data)), "application/json")

def generate_airport_data():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(MINIO_BUCKET): client.make_bucket(MINIO_BUCKET)
    data = [AirportGenerator().generate_airport(i) for i in range(NUM_AIRPORT_RECORDS)]
    client.put_object(MINIO_BUCKET, OBJECT_NAME_AIRPORT, BytesIO(json.dumps(data).encode()), len(json.dumps(data)), "application/json")

def generate_flight_data():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    airlines = load_existing_data(client, OBJECT_NAME_AIRLINE)
    airports = load_existing_data(client, OBJECT_NAME_AIRPORT)
    airline_iatas = [a['iata'] for a in airlines]
    airport_iatas = [a['iata_code'] for a in airports]
    flights = [FlightGenerator(airline_iatas, airport_iatas).generate_flight() for _ in range(NUM_FLIGHT_RECORDS)]
    client.put_object(MINIO_BUCKET, OBJECT_NAME_FLIGHT, BytesIO(json.dumps(flights).encode()), len(json.dumps(flights)), "application/json")

default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}

with DAG(
    dag_id='injest_all_semi_structured_data_from_api_to_minio',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data-generation', 'minio', 'airlines', 'airports', 'flights']
) as dag:

    t1 = PythonOperator(task_id='generate_airline_data', python_callable=generate_airline_data)
    t2 = PythonOperator(task_id='generate_airport_data', python_callable=generate_airport_data)
    t3 = PythonOperator(task_id='generate_flight_data', python_callable=generate_flight_data)

    t1 >> t2 >> t3
