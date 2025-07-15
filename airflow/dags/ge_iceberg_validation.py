from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import great_expectations as ge
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults
)
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
    try:
        # Step 1: Create dummy DataFrame
        df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie", "David"],
            "age": [25, 32, 45, 29],
            "salary": [50000, 60000, 80000, 70000]
        })

        # Step 2: Minimal GE project config
        config = DataContextConfig(
            config_version=3.0,
            stores={
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "/tmp/ge/expectations"
                    }
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "/tmp/ge/validations"
                    }
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                }
            },
            expectations_store_name="expectations_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            validations_store_name="validations_store",
            store_backend_defaults=FilesystemStoreBackendDefaults(
                root_directory="/tmp/ge"
            ),
            data_docs_sites={},
        )

        # Step 3: Create Ephemeral Context from config
        context = EphemeralDataContext(project_config=config)

        # Step 4: Add datasource programmatically
        datasource_config = {
            "name": "my_pandas_datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "PandasExecutionEngine"
            },
            "data_connectors": {
                "runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier"]
                }
            }
        }
        context.add_datasource(**datasource_config)

        # Step 5: Create batch request from DataFrame
        batch_request = RuntimeBatchRequest(
            datasource_name="my_pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="dummy_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier": "dummy"},
        )

        # Step 6: Create suite and validator
        context.create_expectation_suite(
            "dummy_suite", 
            overwrite_existing=True
        )
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="dummy_suite"
        )

        # Step 7: Define expectations
        validator.expect_column_to_exist("name")
        validator.expect_column_values_to_not_be_null("age")
        validator.expect_column_values_to_be_between("age", 18, 65)
        validator.expect_column_mean_to_be_between("salary", 40000, 90000)

        # Step 8: Validate
        results = validator.validate()
        if not results.success:
            failed_expectations = [
                exp.expectation_config.expectation_type 
                for exp in results.results 
                if not exp.success
            ]
            raise ValueError(
                f"Validation failed for expectations: {failed_expectations}"
            )
        print("✅ Great Expectations validation passed!")
        return "Validation succeeded"
        
    except Exception as e:
        print(f"❌ Validation failed with error: {str(e)}")
        raise

run_validation_task = PythonOperator(
    task_id='run_dummy_ge_validation',
    python_callable=run_ge_validation,
    dag=dag,
)
