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
NUM_RECORDS = 500
MAX_RECORDS = 500000
FILE_FORMAT = 'json'  # Options: 'json' or 'csv'
MINIO_ENDPOINT = 'minio:9009'  # Update based on Airflow-MinIO network
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_SECURE = False
MINIO_BUCKET = 'warehouse'
OBJECT_NAME = f"bronze_layer/semi_structured_raw_data/airline_data/airlines_data.{FILE_FORMAT}"
# ====================================================

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


class AirlineGenerator:
    def __init__(self):
        self.used_iata = set()
        self.used_icao = set()
        self._init_code_pools()

    def _init_code_pools(self):
        for a in PREDEFINED_AIRLINES:
            self.used_iata.add(a["iata"])
            self.used_icao.add(a["icao"])

        self.available_iata = [
            f"{a}{b}" for a, b in product(ascii_uppercase, repeat=2)
            if f"{a}{b}" not in self.used_iata
        ]
        random.shuffle(self.available_iata)

        self.available_icao = [
            f"{a}{b}{c}" for a, b, c in product(ascii_uppercase, repeat=3)
            if f"{a}{b}{c}" not in self.used_icao
        ]
        random.shuffle(self.available_icao)

    def _get_codes(self, index):
        if index < len(PREDEFINED_AIRLINES):
            airline = PREDEFINED_AIRLINES[index]
            return airline["iata"], airline["icao"]

        iata = self.available_iata.pop() if self.available_iata else self._fallback_code(2)
        icao = self.available_icao.pop() if self.available_icao else self._fallback_code(3)

        self.used_iata.add(iata)
        self.used_icao.add(icao)
        return iata, icao

    def _fallback_code(self, length):
        while True:
            code = ''.join(random.choices(ascii_uppercase, k=length))
            if length == 2 and code not in self.used_iata:
                return code
            if length == 3 and code not in self.used_icao:
                return code

    def generate_airline(self, index):
        iata, icao = self._get_codes(index)

        if index < len(PREDEFINED_AIRLINES):
            a = PREDEFINED_AIRLINES[index]
            name = a["name"]
            callsign = a["callsign"]
        else:
            country_name = random.choice(list(COUNTRIES.values()))["name"]
            name = f"{country_name.split()[0]} {random.choice(['Air', 'Airways', 'Airlines'])}"
            callsign = name.replace(" ", "").upper()[:8]

        country_code = random.choice(list(COUNTRIES.keys()))
        country = COUNTRIES[country_code]
        hub = random.choice(country["hubs"])

        founded = random.randint(1920, datetime.now().year - 1) if random.random() < 0.7 else random.randint(1981, datetime.now().year - 1)
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


def flatten_dict(d):
    return {k: '|'.join(v) if isinstance(v, list) else v for k, v in d.items()}


def generate_and_upload_data():
    generator = AirlineGenerator()
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    try:
        obj = client.get_object(MINIO_BUCKET, OBJECT_NAME)
        content = obj.read()
        existing_data = json.loads(content) if FILE_FORMAT == 'json' else list(csv.DictReader(content.decode().splitlines()))
    except:
        existing_data = []

    new_data = [generator.generate_airline(i) for i in range(NUM_RECORDS)]
    full_data = (existing_data + new_data)[-MAX_RECORDS:]

    if FILE_FORMAT == 'json':
        byte_data = json.dumps(full_data, indent=2).encode('utf-8')
        content_type = "application/json"
    elif FILE_FORMAT == 'csv':
        flat_data = [flatten_dict(record) for record in full_data]
        fieldnames = list(flat_data[0].keys())
        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flat_data)
        byte_data = buffer.getvalue().encode('utf-8')
        content_type = "text/csv"
    else:
        raise ValueError("Unsupported format")

    client.put_object(
        MINIO_BUCKET,
        OBJECT_NAME,
        data=BytesIO(byte_data),
        length=len(byte_data),
        content_type=content_type
    )
    print(f"✅ Uploaded {len(full_data)} records to {OBJECT_NAME}")


# ========== Airflow DAG Definition ==========
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='injest_airline_data_semi_structured_to_minio',
    default_args=default_args,
    description='Generate Airline Data and Upload to MinIO',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data-generation', 'minio', 'airlines']
) as dag:

    generate_upload_task = PythonOperator(
        task_id='generate_and_upload_airline_data',
        python_callable=generate_and_upload_data
    )
