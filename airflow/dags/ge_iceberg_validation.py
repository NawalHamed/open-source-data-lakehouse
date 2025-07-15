from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as gx

def run_gx_on_dataframe():
    # 1️⃣  Sample Data
    df = pd.DataFrame(
        {
            "name":  ["Ali", "Sara", "John", "Ali"],
            "age":   [25, 30, 22, 25],
            "email": ["ali@example.com",
                      "sara@example.com",
                      "john@example.com",
                      "ali@example.com"],
        }
    )
    print("🔵 DataFrame to validate:\n", df)

    # 2️⃣  Ephemeral GE context
    context = gx.get_context(mode="ephemeral")

    # 3️⃣  Create / fetch an empty expectation-suite
    suite_name = "demo_suite"
    try:
        context.get_expectation_suite(suite_name)
    except gx.exceptions.DataContextError:
        context.add_or_update_expectation_suite(suite_name)

    # 4️⃣  Get a validator *with* an active batch from the in-memory DataFrame
    validator = context.get_validator(
        batch_data=df,
        expectation_suite_name=suite_name,
    )

    # 5️⃣  Add expectations
    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_be_unique("email")
    validator.expect_column_values_to_be_between("age", min_value=20, max_value=40)

    # 6️⃣  Persist the suite (still in-memory in an EphemeralContext)
    validator.save_expectation_suite(discard_failed_expectations=False)

    # 7️⃣  Validate and print a concise summary
    results = validator.validate()
    print("✅ Validation overall success:", results.success)
    for res in results.results:
        etype = res.expectation_config.expectation_type
        col   = res.expectation_config.kwargs.get("column", "-")
        print(
            f"  • {etype} on '{col}' ⇒ ",
            "PASSED" if res.success else "FAILED",
        )

    # 8️⃣  Fail the task if any expectation failed
    if not results.success:
        raise Exception("❌ Data quality check failed – see log for details")

# ────────────────────────────────────────────────────────────────────────────────
default_args = {"start_date": datetime(2025, 7, 15), "catchup": False}

with DAG(
    dag_id="gx_dataframe_validation_dag",
    default_args=default_args,
    schedule_interval=None,
    description="Validate a Pandas DataFrame with Great Expectations 1.1.x",
    tags=["gx", "pandas", "validation"],
) as dag:

    PythonOperator(
        task_id="run_gx_validation",
        python_callable=run_gx_on_dataframe,
    )
