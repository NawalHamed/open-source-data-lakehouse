from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from great_expectations.dataset import PandasDataset

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ge_dummy_df_validation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def run_ge_validation():
    # Step 1: Create dummy DataFrame
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 32, 45, 29],
        "salary": [50000, 60000, 80000, 70000]
    })

    # Step 2: Convert to GE Dataset
    ge_df = PandasDataset(df)

    # Step 3: Add Expectations
    ge_df.expect_column_to_exist("name")
    ge_df.expect_column_values_to_not_be_null("age")
    ge_df.expect_column_values_to_be_between("age", min_value=18, max_value=65)
    ge_df.expect_column_mean_to_be_between("salary", min_value=40000, max_value=90000)

    # Step 4: Run validation
    result = ge_df.validate()

    if not result.success:
        raise ValueError("❌ Great Expectations validation failed.")
    else:
        print("✅ Great Expectations validation passed!")

# Step 5: Add Airflow task
run_validation_task = PythonOperator(
    task_id='run_dummy_ge_validation',
    python_callable=run_ge_validation,
    dag=dag,
)
