from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import EphemeralDataContext

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'ge_iceberg_validation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run Great Expectations validation on Iceberg table',
)

def validate_iceberg_table():
    # 1. Create sample dataframe (replace with Trino query if needed)
    df = pd.DataFrame({
        "flight_id": [101, 102, None, 104],
        "airline": ["WY", "QR", "EK", "BA"],
        "distance_km": [700, 1500, 900, None]
    })

    # 2. Create an in-memory context
    context = EphemeralDataContext()

    # 3. Define batch request using in-memory dataframe
    batch_request = RuntimeBatchRequest(
        datasource_name="my_pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="my_airflow_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": "airflow_validation_001"}
    )

    # 4. Add a simple in-memory Pandas datasource
    context.add_datasource(
        name="my_pandas_datasource",
        class_name="Datasource",
        execution_engine={
            "class_name": "PandasExecutionEngine"
        },
        data_connectors={
            "runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["run_id"]
            }
        }
    )

    # 5. Create expectation suite
    suite_name = "airflow_suite"
    context.create_expectation_suite(expectation_suite_name=suite_name, overwrite_existing=True)

    # 6. Get validator and define expectations
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    validator.expect_column_values_to_not_be_null("flight_id")
    validator.expect_column_values_to_not_be_null("distance_km")
    validator.expect_column_values_to_be_in_set("airline", ["WY", "QR", "EK", "BA", "LH", "TK"])

    # 7. Run validation
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[validator]
    )

    # 8. Check result
    if not results["success"]:
        raise ValueError("❌ Validation failed.")
    else:
        print("✅ Validation passed.")

# Python Operator
validation_task = PythonOperator(
    task_id='run_ge_check',
    python_callable=validate_iceberg_table,
    dag=dag
)

validation_task
