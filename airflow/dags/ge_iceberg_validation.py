from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

def run_gx_on_dataframe():
    # Step 1: Create DataFrame
    df = pd.DataFrame({
        "name": ["Ali", "Sara", "John", "Ali"],
        "age": [25, 30, 22, 25],
        "email": ["ali@example.com", "sara@example.com", "john@example.com", "ali@example.com"]
    })
    print("📦 DataFrame:\n", df)

    # Step 2: Ephemeral context
    context = gx.get_context(mode="ephemeral")

    # Step 3: Create ExpectationSuite object (no context method)
    suite = ExpectationSuite("demo_suite")

    # Step 4: Get validator directly using DataFrame + suite
    validator = context.get_validator(
        batch_data=df,
        expectation_suite=suite
    )

    # Step 5: Define expectations
    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_be_unique("email")
    validator.expect_column_values_to_be_between("age", min_value=20, max_value=40)

    # Step 6: Validate
    results = validator.validate()

    # Step 7: Output
    print("✅ Validation success:", results.success)
    for res in results.results:
        exp = res.expectation_config.expectation_type
        col = res.expectation_config.kwargs.get("column", "-")
        print(f"  → {exp} on '{col}': {'PASSED ✅' if res.success else 'FAILED ❌'}")

    if not results.success:
        raise Exception("❌ Validation failed!")

# ─────────────────────────────────────────────────────────────
default_args = {"start_date": datetime(2025, 7, 15), "catchup": False}

with DAG(
    dag_id="gx_dataframe_validation_dag",
    default_args=default_args,
    schedule_interval=None,
    tags=["gx", "pandas", "validation"],
    description="Validate DataFrame using GE 1.1.0 in Airflow with EphemeralContext"
) as dag:

    validate_task = PythonOperator(
        task_id="run_gx_validation",
        python_callable=run_gx_on_dataframe
    )

    validate_task
