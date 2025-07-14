from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

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
    # 1️⃣ Create dummy DataFrame
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 32, 45, 29],
        "salary": [50000, 60000, 80000, 70000]
    })

    # 2️⃣ Load existing GE project context
    context = ge.get_context()

    # 3️⃣ Ensure a basic Pandas datasource is registered
    context.sources.add_pandas(name="my_pandas_datasource")

    # 4️⃣ Create or update suite
    suite_name = "dummy_suite"
    context.add_or_update_expectation_suite(suite_name)

    # 5️⃣ Build batch request
    batch_request = RuntimeBatchRequest(
        datasource_name="my_pandas_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="dummy_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier": "default"},
    )

    # 6️⃣ Get validator and add expectations
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    validator.expect_column_to_exist("name")
    validator.expect_column_values_to_not_be_null("age")
    validator.expect_column_values_to_be_between("age", 18, 65)
    validator.expect_column_mean_to_be_between("salary", 40000, 90000)

    # 7️⃣ Run validation
    results = validator.validate()
    if not results.success:
        raise ValueError("❌ Validation failed.")
    print("✅ Validation passed!")

run_validation_task = PythonOperator(
    task_id='run_dummy_ge_validation',
    python_callable=run_ge_validation,
    dag=dag,
)
