from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import great_expectations as ge
from great_expectations.data_context.types.base import DataContextConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context import EphemeralDataContext
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
    # Step 1: Create dummy DataFrame
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [25, 32, 45, 29],
        "salary": [50000, 60000, 80000, 70000]
    })

    # Step 2: Minimal GE project config
    config = DataContextConfig(
        store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/tmp/ge"),
        datasources={
            "my_pandas_datasource": {
                "execution_engine": {"class_name": "PandasExecutionEngine"},
                "data_connectors": {
                    "runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier"],
                    }
                },
            }
        },
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        data_docs_sites={},
    )

    # Step 3: Create Ephemeral Context from config
    context = EphemeralDataContext(project_config=config)

    # Step 4: Create batch request from DataFrame
    batch_request = RuntimeBatchRequest(
        datasource_name="my_pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="dummy_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier": "dummy"},
    )

    # Step 5: Create suite and validator
    context.create_expectation_suite("dummy_suite", overwrite_existing=True)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="dummy_suite"
    )

    # Step 6: Define expectations
    validator.expect_column_to_exist("name")
    validator.expect_column_values_to_not_be_null("age")
    validator.expect_column_values_to_be_between("age", 18, 65)
    validator.expect_column_mean_to_be_between("salary", 40000, 90000)

    # Step 7: Validate
    results = validator.validate()
    if not results.success:
        raise ValueError("❌ Validation failed.")
    print("✅ Great Expectations validation passed!")

run_validation_task = PythonOperator(
    task_id='run_dummy_ge_validation',
    python_callable=run_ge_validation,
    dag=dag,
)
