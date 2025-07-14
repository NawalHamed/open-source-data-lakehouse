from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import great_expectations as ge
from great_expectations.checkpoint import CheckpointResult
from great_expectations.data_context import FileDataContext

# ========== DAG CONFIG ==========
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ge_dummy_df_validation',
    default_args=default_args,
    description='Validate dummy DataFrame using Great Expectations',
    schedule_interval=None,
    catchup=False,
)

# ========== TASK FUNCTION ==========
def run_ge_validation(**kwargs):
    # Dummy DataFrame
    data = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 32, 45, 29],
        "salary": [50000, 60000, 80000, 70000]
    })

    # Convert to GE Dataset
    ge_df = ge.from_pandas(data)

    # Add expectations
    ge_df.expect_column_to_exist("name")
    ge_df.expect_column_values_to_not_be_null("age")
    ge_df.expect_column_values_to_be_between("age", min_value=18, max_value=65)
    ge_df.expect_column_mean_to_be_between("salary", min_value=40000, max_value=90000)

    # Run validation
    result = ge_df.validate()
    
    # Optional: Raise error if validation fails
    if not result.success:
        raise ValueError("Validation failed!")

    print("Great Expectations validation passed ✅")

# ========== TASK ==========
run_validation_task = PythonOperator(
    task_id='run_dummy_ge_validation',
    python_callable=run_ge_validation,
    provide_context=True,
    dag=dag,
)

run_validation_task
