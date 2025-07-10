import pandas as pd
import great_expectations as ge
from sqlalchemy.engine import create_engine

# Step 1: Connect to Trino (Make sure Trino is running and Iceberg is configured)
TRINO_HOST = "trino"  # Docker service name or IP
TRINO_PORT = 8080
CATALOG = "iceberg"  # This is the Nessie Iceberg catalog in Trino
SCHEMA = "gold_layer"
TABLE = "flight_performance_summary_v1"

engine = create_engine(
    f'trino://user@{TRINO_HOST}:{TRINO_PORT}/{CATALOG}/{SCHEMA}'
)

# Step 2: Query the Iceberg table via Trino
query = f"SELECT * FROM {TABLE} LIMIT 100"
df = pd.read_sql(query, engine)

# Step 3: Convert to GE DataFrame
ge_df = ge.from_pandas(df)

# Step 4: Define Expectations
ge_df.expect_column_to_exist("flight_number")
ge_df.expect_column_values_to_not_be_null("flight_date")
ge_df.expect_column_values_to_be_between("distance_km", min_value=0, max_value=20000)
ge_df.expect_column_values_to_match_regex("flight_number", r"^[A-Z]{2}\d{3,4}$", mostly=0.9)

# Step 5: Validate
results = ge_df.validate()

# Step 6: Print Results
import json
print(json.dumps(results, indent=2))
