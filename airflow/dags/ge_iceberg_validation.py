from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as ge

# Optional: if you're querying from Trino
# from sqlalchemy.engine import create_engine
# engine = create_engine("trino://user@trino:8080/hive/iceberg")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'ge_iceberg_validation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run Great Expectations validation on Iceberg table'
)

def validate_iceberg_table():
    # Option 1: Load data from a mock DataFrame
    df = pd.DataFrame({
        "flight_id": [101, 102, None, 104],
        "airline": ["WY", "QR", "EK", "BA"],
        "distance_km": [700, 1500, 900, None]
    })

    # Option 2: Load via SQL from Trino
    # df = pd.read_sql("SELECT * FROM iceberg.gold_layer.flight_summary LIMIT 100", con=engine)

    # Create GE DataFrame
    ge_df = ge.from_pandas(df)

    # Define Expectations
    results = []

    results.append(ge_df.expect_column_values_to_not_be_null("flight_id"))
    results.append(ge_df.expect_column_values_to_not_be_null("distance_km"))
    results.append(ge_df.expect_column_values_to_be_in_set("airline", ["WY", "QR", "EK", "BA", "LH", "TK"]))

    # Check if all expectations passed
    all_passed = all([r["success"] for r in results])
    if not all_passed:
        raise ValueError("❌ One or more data quality checks failed.")
    else:
        print("✅ All data quality checks passed.")

# Define PythonOperator
validation_task = PythonOperator(
    task_id='run_ge_check',
    python_callable=validate_iceberg_table,
    dag=dag
)

validation_task
