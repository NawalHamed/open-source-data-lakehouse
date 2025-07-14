from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import EphemeralDataContext

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
    # Create dummy DataFrame
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 32, 45, 29],
        "salary": [50000, 60000, 80000, 70000]
    })

    # Create a simple in-memory Great Expectations Data Context
    context = EphemeralDataContext()

    # Define a Datasource
    context.sources.add_pandas(name="my_pandas_datasource")

    # Add an Expectation Suite
    context.add_or_update_expectation_suite("dummy_suite")

    # Create Validator with DataFrame
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_pandas_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="dummy_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier": "default"},
        ),
        expectation_suite_name="dummy_suite",
    )

    # Add Expectations
    validator.expect_column_to_exist("name")
    validator.expect_column_values_to_not_be_null("age")
    validator.expect_column_values_to_be_between("age", 18, 65)
    validator.expect_column_mean_to_be_between("salary", 40000, 90000)

    # Validate
    results = validator.validate()

    if not results.success:
        raise ValueError("❌ Great Expectations validation failed.")
    else:
        print("✅ Great Expectations validation passed!")

run_validation_task = PythonOperator(
    task_id='run_dummy_ge_validation',
    python_callable=run_ge_validation,
    dag=dag,
)
