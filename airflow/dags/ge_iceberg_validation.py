from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as gx
from great_expectations.validator.validator import Validator
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.execution_engine import PandasExecutionEngine

def run_gx_on_dataframe():
    print("🔵 Creating sample DataFrame...")
    df = pd.DataFrame({
        "name": ["Ali", "Sara", "John", "Ali"],
        "age": [25, 30, 22, 25],
        "email": ["ali@example.com", "sara@example.com", "john@example.com", "ali@example.com"]
    })
    print(df)

    print("🔵 Initializing Ephemeral Great Expectations context...")
    context = gx.get_context(mode="ephemeral")

    print("📘 Manually creating an expectation suite object...")
    suite = ExpectationSuite("demo_suite")

    print("🔎 Creating validator with PandasExecutionEngine and in-memory suite...")
    validator = Validator(
        execution_engine=PandasExecutionEngine(),
        data=df,
        expectation_suite=suite
    )

    print("🧪 Applying expectations...")
    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_be_unique("email")
    validator.expect_column_values_to_be_between("age", min_value=20, max_value=40)

    print("🚦 Running validation...")
    results = validator.validate()

    print("✅ Validation success:", results.success)
    print("🔍 Detailed Results:")
    for r in results.results:
        expectation = r.expectation_config.expectation_type
        column = r.expectation_config.kwargs.get("column", "N/A")
        print(f"  - Expectation: {expectation}, Column: {column}, Passed: {r.success}")

    if not results.success:
        raise Exception("❌ Data validation failed.")

# Airflow DAG setup
default_args = {
    'start_date': datetime(2025, 7, 15),
    'catchup': False
}

with DAG(
    dag_id='gx_dataframe_validation_dag',
    default_args=default_args,
    schedule_interval=None,
    description='Run GE v1.1.0 on a Pandas DataFrame with EphemeralContext',
    tags=['gx', 'pandas', 'validation']
) as dag:

    validate_task = PythonOperator(
        task_id='run_gx_validation',
        python_callable=run_gx_on_dataframe
    )

    validate_task
