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

# ============== CONFIGURATION =================
MINIO_ENDPOINT = 'minio:9009'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'lakehouse'

# Master data paths
MASTER_AIRLINE_PATH = "bronze_layer/master/airlines_data.json"
MASTER_AIRPORT_PATH = "bronze_layer/master/airports_data.json"
MASTER_COUNTRY_PATH = "bronze_layer/master/countries_data.csv"
MASTER_CITY_PATH = "bronze_layer/master/cities_data.csv"

COUNTRIES = {
    "US": {"name": "United States", "hubs": ["ATL", "DFW", "ORD", "LAX", "JFK"]},
    "DE": {"name": "Germany", "hubs": ["FRA", "MUC", "TXL"]},
    "FR": {"name": "France", "hubs": ["CDG", "ORY", "LYS"]},
    "JP": {"name": "Japan", "hubs": ["NRT", "HND"]},
    "GB": {"name": "United Kingdom", "hubs": ["LHR", "LGW", "MAN"]},
    "IN": {"name": "India", "hubs": ["DEL", "BOM", "BLR"]},
}

CONTINENTS = ["AF", "EU", "AS", "NA", "SA", "OC", "AN"]

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

class CountryGenerator:
    def __init__(self):
        self.used_iso2 = set(COUNTRIES.keys())
        
    def generate_country(self, index, timestamp):
        while True:
            iso2 = ''.join(random.choices(ascii_uppercase, k=2))
            if iso2 not in self.used_iso2:
                self.used_iso2.add(iso2)
                break
                
        name = f"Country {index}"
        return {
            "id": str(3000000 + index),
            "name": name,
            "iso2": iso2,
            "capital": f"Capital {index}",
            "continent": random.choice(CONTINENTS),
            "population": random.randint(1000000, 1000000000),
            "created_at": timestamp,
            "updated_at": timestamp
        }

class CityGenerator:
    def __init__(self):
        self.available_iata = [f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3)]
        random.shuffle(self.available_iata)
        
    def generate_city(self, index, timestamp):
        iata = self.available_iata.pop()
        country_code = random.choice(list(COUNTRIES.keys()))
        return {
            "id": str(4000000 + index),
            "iata_code": iata,
            "city_name": f"City {index}",
            "country_iso2": country_code,
            "latitude": round(random.uniform(-90, 90), 6),
            "longitude": round(random.uniform(-180, 180), 6),
            "timezone": "UTC",
            "created_at": timestamp,
            "updated_at": timestamp
        }

# ============== HELPERS ==============
def load_existing_json_data(client, object_name):
    try:
        obj = client.get_object(MINIO_BUCKET, object_name)
        return json.loads(obj.read())
    except:
        return []

def load_existing_csv_data(client, object_name):
    try:
        obj = client.get_object(MINIO_BUCKET, object_name)
        csv_data = obj.read().decode('utf-8').splitlines()
        return list(csv.DictReader(csv_data))
    except:
        return []

def save_as_json(client, object_name, data):
    client.put_object(
        MINIO_BUCKET, object_name,
        BytesIO(json.dumps(data).encode()),
        len(json.dumps(data)),
        "application/json"
    )

def save_as_csv(client, object_name, data):
    if not data:
        return
        
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    
    client.put_object(
        MINIO_BUCKET, object_name,
        BytesIO(output.getvalue().encode()),
        len(output.getvalue()),
        "text/csv"
    )

def update_record(client, master_path, record_id, updates, is_csv=False):
    try:
        data = load_existing_csv_data(client, master_path) if is_csv else load_existing_json_data(client, master_path)
        updated = False
        
        for record in data:
            if str(record['id']) == str(record_id):
                record.update(updates)
                record['updated_at'] = datetime.utcnow().isoformat()
                updated = True
                break
        
        if updated:
            if is_csv:
                save_as_csv(client, master_path, data)
            else:
                save_as_json(client, master_path, data)
            return True
        return False
    except Exception as e:
        print(f"Error updating record: {e}")
        return False

# ============== ADDITION TASKS ==============
def add_new_airline():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_json_data(client, MASTER_AIRLINE_PATH)
    now = datetime.utcnow().isoformat()
    ag = AirlineGenerator()
    
    existing_iatas = {a["iata"] for a in data}
    ag.available_iata = [code for code in ag.available_iata if code not in existing_iatas]
    random.shuffle(ag.available_iata)

    new_airline = ag.generate_airline(len(data), now)
    data.append(new_airline)

    save_as_json(client, MASTER_AIRLINE_PATH, data)
    print(f"✅ Added new airline: {new_airline['name']} ({new_airline['iata']})")

def add_new_airport():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_json_data(client, MASTER_AIRPORT_PATH)
    now = datetime.utcnow().isoformat()
    ag = AirportGenerator()
    
    existing_iatas = {a["iata_code"] for a in data}
    ag.available_iata = [code for code in ag.available_iata if code not in existing_iatas]
    random.shuffle(ag.available_iata)

    new_airport = ag.generate_airport(len(data), now)
    data.append(new_airport)

    save_as_json(client, MASTER_AIRPORT_PATH, data)
    print(f"✅ Added new airport: {new_airport['airport_name']} ({new_airport['iata_code']})")

def add_new_country():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_csv_data(client, MASTER_COUNTRY_PATH)
    now = datetime.utcnow().isoformat()
    cg = CountryGenerator()

    new_country = cg.generate_country(len(data), now)
    data.append(new_country)

    save_as_csv(client, MASTER_COUNTRY_PATH, data)
    print(f"✅ Added new country: {new_country['name']} ({new_country['iso2']})")

def add_new_city():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_csv_data(client, MASTER_CITY_PATH)
    now = datetime.utcnow().isoformat()
    cg = CityGenerator()
    
    existing_iatas = {a["iata_code"] for a in data}
    cg.available_iata = [code for code in cg.available_iata if code not in existing_iatas]
    random.shuffle(cg.available_iata)

    new_city = cg.generate_city(len(data), now)
    data.append(new_city)

    save_as_csv(client, MASTER_CITY_PATH, data)
    print(f"✅ Added new city: {new_city['city_name']} ({new_city['iata_code']})")

# ============== UPDATE TASKS ==============
def update_airline():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_json_data(client, MASTER_AIRLINE_PATH)
    if data:
        record = random.choice(data)
        updates = {
            'status': random.choice(['active', 'inactive', 'bankrupt']),
            'hub': random.choice(list(COUNTRIES.values()))['hubs'][0]
        }
        if update_record(client, MASTER_AIRLINE_PATH, record['id'], updates, is_csv=False):
            print(f"✅ Updated airline {record['id']} with {updates}")

def update_airport():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_json_data(client, MASTER_AIRPORT_PATH)
    if data:
        record = random.choice(data)
        updates = {
            'airport_name': f"Updated {record['airport_name']}",
            'country_iso2': random.choice(list(COUNTRIES.keys()))
        }
        if update_record(client, MASTER_AIRPORT_PATH, record['id'], updates, is_csv=False):
            print(f"✅ Updated airport {record['id']} with {updates}")

def update_country():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_csv_data(client, MASTER_COUNTRY_PATH)
    if data:
        record = random.choice(data)
        updates = {
            'population': random.randint(1000000, 1000000000),
            'capital': f"New {record['capital']}"
        }
        if update_record(client, MASTER_COUNTRY_PATH, record['id'], updates, is_csv=True):
            print(f"✅ Updated country {record['id']} with {updates}")

def update_city():
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    data = load_existing_csv_data(client, MASTER_CITY_PATH)
    if data:
        record = random.choice(data)
        updates = {
            'timezone': random.choice(['UTC+1', 'UTC+2', 'UTC-5']),
            'latitude': round(random.uniform(-90, 90), 6),
            'longitude': round(random.uniform(-180, 180), 6)
        }
        if update_record(client, MASTER_CITY_PATH, record['id'], updates, is_csv=True):
            print(f"✅ Updated city {record['id']} with {updates}")

# ============== DAG CONFIGURATION ==============
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='master_data_management',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['minio', 'master-data', 'management']
) as dag:

    # Addition tasks
    add_airline = PythonOperator(task_id='add_airline', python_callable=add_new_airline)
    add_airport = PythonOperator(task_id='add_airport', python_callable=add_new_airport)
    add_country = PythonOperator(task_id='add_country', python_callable=add_new_country)
    add_city = PythonOperator(task_id='add_city', python_callable=add_new_city)

    # Update tasks
    upd_airline = PythonOperator(task_id='update_airline', python_callable=update_airline)
    upd_airport = PythonOperator(task_id='update_airport', python_callable=update_airport)
    upd_country = PythonOperator(task_id='update_country', python_callable=update_country)
    upd_city = PythonOperator(task_id='update_city', python_callable=update_city)

    # Option 1: Separate addition and update flows
    addition_flow = add_airline >> add_airport >> add_country >> add_city
  #  update_flow = upd_airline >> upd_airport >> upd_country >> upd_city

    # Set the dependencies
    addition_flow
  #  update_flow
