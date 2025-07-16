from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.exceptions import AirflowException # No longer needed if not raising exceptions
from datetime import datetime

import pandas as pd
import great_expectations as gx

def run_great_expectations_validation():
    """
    This function encapsulates the Great Expectations validation logic.
    It creates a dummy DataFrame, defines expectations, and runs the validation.
    It will print the validation results but will NOT raise an AirflowException
    even if validation fails, ensuring the Airflow task always succeeds.
    """
    # Create sample DataFrame
    df = pd.DataFrame({
        "passenger_id": [1, 2, 3, 4],
        "passenger_count": [1, 3, 2, 7],
        "trip_distance_km": [5.2, 3.8, 7.1, 1.4]
    })

    # Initialize Great Expectations Validator from Pandas DataFrame
    validator = gx.from_pandas(df)

    # Add expectations
    print("Adding Great Expectations...")
    # Expect 'passenger_count' values to be between 1 and 6 (inclusive)
    validator.expect_column_values_to_be_between("passenger_count", min_value=1, max_value=6)
    # Expect 'trip_distance_km' column to not contain any null values
    validator.expect_column_values_to_not_be_null("trip_distance_km")

    # Run validation
    print("Running Great Expectations validation...")
    results = validator.validate()

    # Print validation results summary
    print("\nGreat Expectations Validation Results:")
    print(f"Success: {results.success}")

    if not results.success:
        print("\nDetailed Failures:")
        for result in results.results:
            if not result.success:
                print(f"- Expectation: {result.expectation_config.expectation_type}")
                print(f"  Column: {result.expectation_config.kwargs.get('column')}")
                print(f"  Details: {result.result}")
        print("\nGreat Expectations validation completed with failures, but task will succeed as requested.")
    else:
        print("\nAll Great Expectations passed successfully!")

# Define the Airflow DAG
with DAG(
    dag_id='great_expectations_dataframe_validation_always_succeed', # Changed DAG ID to reflect new behavior
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_quality', 'great_expectations', 'pandas', 'non_failing'],
    doc_md="""
    ### Great Expectations DataFrame Validation DAG (Always Succeed)
    This DAG demonstrates how to integrate Great Expectations with Apache Airflow
    to validate a Pandas DataFrame.
    The `run_great_expectations_validation` task will always complete successfully,
    even if some Great Expectations are not met. The validation results will be
    available in the task logs.
    """
) as dag:
    # Define the PythonOperator task
    validate_dataframe_task = PythonOperator(
        task_id='validate_dummy_dataframe_non_failing', # Changed task ID
        python_callable=run_great_expectations_validation,
    )
