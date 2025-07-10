from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import json
from sqlalchemy.engine import create_engine
from great_expectations.dataset import PandasDataset

def validate_flight_data():
    # Step 1: Connect to Trino
    engine = create_engine("trino://user@trino:8080/iceberg/gold_layer")

    # Step 2: Load data from Iceberg table via Trino
    query = "SELECT * FROM flight_performance_summary_v1 LIMIT 100"
    df = pd.read_sql(query, engine)

    # Step 3: Wrap with Great Expectations PandasDataset
    ge_df = PandasDataset(df)

    # Step 4: Define expectations
    ge_df.expect_column_to_exist("flight_number")
    ge_df.expect_column_values_to_not_be_null("flight_date")
    ge_df.expect_column_values_to_be_between("distance_km", min_value=0, max_value=20000)
    ge_df.expect_column_values_to_match_regex("flight_number", r"^[A-Z]{2}\d{3,4}$", mostly=0.9)

    # Step 5: Validate
    result = ge_df.validate()

    # Step 6: Print results
    print(json.dumps(result, indent=2))

default_args = {
    'start_date': datetime(2025, 7, 10),
    'catchup': False,
}

with DAG(
    dag_id='ge_iceberg_validation',
    default_args=default_args,
    schedule_interval=None,
    tags=['great_expectations', 'trino', 'iceberg']
) as dag:

    run_ge_check = PythonOperator(
        task_id='run_ge_check',
        python_callable=validate_flight_data
    )
