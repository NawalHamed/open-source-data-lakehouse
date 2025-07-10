from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as ge
from sqlalchemy.engine import create_engine
import json

def validate_flight_data():
    # Connect to Trino (assuming Trino is running in the same Docker network)
    engine = create_engine("trino://user@trino:8080/iceberg/gold_layer")

    query = "SELECT * FROM flight_performance_summary_v1 LIMIT 100"
    df = pd.read_sql(query, engine)

    # Convert to GE DataFrame
    ge_df = ge.from_pandas(df)

    # Add expectations
    ge_df.expect_column_to_exist("flight_number")
    ge_df.expect_column_values_to_not_be_null("flight_date")
    ge_df.expect_column_values_to_be_between("distance_km", 0, 20000)
    ge_df.expect_column_values_to_match_regex("flight_number", r"^[A-Z]{2}\d{3,4}$", mostly=0.9)

    # Validate and print results
    result = ge_df.validate()
    print(json.dumps(result, indent=2))


default_args = {
    'start_date': datetime(2025, 7, 10),
    'catchup': False,
}

with DAG('ge_iceberg_validation',
         schedule_interval=None,
         default_args=default_args,
         tags=['ge', 'iceberg']) as dag:

    run_ge_check = PythonOperator(
        task_id='run_ge_check',
        python_callable=validate_flight_data
    )
