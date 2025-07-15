from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as gx

def run_gx_on_dataframe():
    print("🔵 Creating sample DataFrame...")
    df = pd.DataFrame({
        "name": ["Ali", "Sara", "John", "Ali"],
        "age": [25, 30, 22, 25],
        "email": ["ali@example.com", "sara@example.com", "john@example.com", "ali@example.com"]
    })
    print(df)

    print("🔵 Initializing Great Expectations Ephemeral Context...")
    context = gx.get_context(mode="ephemeral")

    print("🔵 Adding Pandas DataFrame as a datasource...")
    datasource = context.sources.add_pandas(name="my_pandas_datasource")

    print("🔵 Creating a data asset from the dataframe...")
    data_asset = datasource.add_dataframe_asset(name="sample_df", dataframe=df)

    print("🔵 Building batch request...")
    batch_request = data_asset.build_batch_request()

    print("🔵 Creating expectation suite...")
    suite = context.add_or_update_expectation_suite("demo_suite")

    print("🔵 Getting validator and applying expectations...")
    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_be_unique("email")
    validator.expect_column_values_to_be_between("age", min_value=20, max_value=40)

    print("✅ Expectations applied:")
    for exp in validator.get_expectation_suite().expectations:
        print(f"  - {exp.expectation_type} on {exp.kwargs}")

    print("💾 Saving expectation suite...")
    validator.save_expectation_suite(discard_failed_expectations=False)

    print("🚦 Running checkpoint...")
    checkpoint_result = context.run_checkpoint(
        name="demo_checkpoint",
        validations=[{
            "batch_request": batch_request,
            "expectation_suite_name": suite.name,
        }]
    )

    success = checkpoint_result["success"]
    print("✅ Checkpoint result: ", "PASSED" if success else "FAILED")
    print("🔍 Full Validation Results Summary:")
    for res in checkpoint_result["run_results"].values():
        for vres in res["validation_result"]["results"]:
            col = vres["expectation_config"]["kwargs"].get("column")
            passed = vres["success"]
            print(f"  - Column: {col} | Passed: {passed} | Expectation: {vres['expectation_config']['expectation_type']}")

    if not success:
        raise Exception("❌ Data validation failed. Check expectations.")

default_args = {
    'start_date': datetime(2025, 7, 15),
    'catchup': False
}

with DAG(
    dag_id='gx_dataframe_validation_dag',
    default_args=default_args,
    schedule_interval=None,
    description='Run Great Expectations on a Pandas DataFrame using Ephemeral Context',
    tags=['gx', 'pandas', 'validation']
) as dag:

    validate_data = PythonOperator(
        task_id='run_gx_validation',
        python_callable=run_gx_on_dataframe
    )

    validate_data
