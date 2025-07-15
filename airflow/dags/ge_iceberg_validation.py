from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as gx

def run_gx_on_dataframe():
    # Step 1: Create DataFrame
    df = pd.DataFrame({
        "name": ["Ali", "Sara", "John", "Ali"],
        "age": [25, 30, 22, 25],
        "email": ["ali@example.com", "sara@example.com", "john@example.com", "ali@example.com"]
    })
    print("📦 DataFrame:\n", df)

    # Step 2: Initialize Ephemeral GE Context
    context = gx.get_context(mode="ephemeral")

    # Step 3: Create and register suite correctly
    suite_name = "demo_suite"
    context.create_expectation_suite(suite_name, overwrite_existing=True)

    # Step 4: Get validator using batch_data and suite name
    validator = context.get_validator(
        batch_data=df,
        expectation_suite_name=suite_name
    )

    # Step 5: Add expectations
    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_be_unique("email")
    validator.expect_column_values_to_be_between("age", min_value=20, max_value=40)

    # Step 6: Validate
    results = validator.validate()

    # Step 7: Print results
    print("✅ Validation Success:", results.success)
    for r in results.results:
        col = r.expectation_config.kwargs.get("column", "-")
        exp = r.expectation_config.expectation_type
        print(f"  → {exp} on '{col}': {'✅ PASSED' if r.success else '❌ FAILED'}")

    if not results.success:
        raise Exception("❌ One or more expectations failed.")

# ─────────────────────────────────────────────────────────────
default_args = {"start_date": datetime(2025, 7, 15), "catchup": False}

with DAG(
    dag_id="gx_dataframe_validation_dag",
    default_args=default_args,
    schedule_interval=None,
    tags=["gx", "pandas", "validation"],
    description="Validate Pandas DataFrame with Great Expectations v1.1.0 and Ephemeral Context"
) as dag:

    validate_task = PythonOperator(
        task_id="run_gx_validation",
        python_callable=run_gx_on_dataframe
    )

    validate_task
