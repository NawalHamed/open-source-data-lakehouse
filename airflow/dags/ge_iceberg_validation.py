from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as gx

def run_gx_on_dataframe():
    # 1️⃣ Sample Data
    df = pd.DataFrame({
        "name": ["Ali", "Sara", "John", "Ali"],
        "age": [25, 30, 22, 25],
        "email": ["ali@example.com", "sara@example.com", "john@example.com", "ali@example.com"]
    })
    print("📦 DataFrame:\n", df)

    # 2️⃣ Ephemeral GE Context
    context = gx.get_context(mode="ephemeral")

    # 3️⃣ Create Expectation Suite
    suite_name = "demo_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    # 4️⃣ Get Validator (this handles batch creation internally)
    validator = context.get_validator(
        batch_data=df,
        expectation_suite_name=suite_name
    )

    # 5️⃣ Add Expectations
    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_be_unique("email")
    validator.expect_column_values_to_be_between("age", min_value=20, max_value=40)

    # 6️⃣ Save suite (in-memory)
    validator.save_expectation_suite(discard_failed_expectations=False)

    # 7️⃣ Run validation
    results = validator.validate()

    # 8️⃣ Print result summary
    print("✅ Validation Success:", results.success)
    for res in results.results:
        col = res.expectation_config.kwargs.get("column", "-")
        exp = res.expectation_config.expectation_type
        print(f"  → {exp} on '{col}':", "PASSED ✅" if res.success else "FAILED ❌")

    if not results.success:
        raise Exception("❌ Data validation failed.")

# ─────────────────────────────────────────────────────────────
default_args = {"start_date": datetime(2025, 7, 15), "catchup": False}

with DAG(
    dag_id="gx_dataframe_validation_dag",
    default_args=default_args,
    schedule_interval=None,
    tags=["gx", "pandas", "validation"],
    description="Validate DataFrame with GE 1.1.0 in Airflow using Ephemeral Context"
) as dag:

    validate_task = PythonOperator(
        task_id="run_gx_validation",
        python_callable=run_gx_on_dataframe
    )

    validate_task
