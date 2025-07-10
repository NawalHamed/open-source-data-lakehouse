from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.batch import RuntimeBatchRequest
import great_expectations as ge
from pyspark.sql import SparkSession

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
    description='Run Great Expectations validation on Spark dataframe (Iceberg table)',
)

def validate_iceberg_table_spark():
    # 1️⃣ Create Spark session
    spark = SparkSession.builder \
        .appName("GE_Iceberg_Validation") \
        .master("local[*]") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9009") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    # 2️⃣ Load Iceberg table
    df = spark.read.format("iceberg").load("nessie.gold_layer.flight_performance_summary_v1")

    # 3️⃣ Define GE project config manually
    project_config = {
        "datasources": {
            "my_spark_datasource": {
                "execution_engine": {"class_name": "SparkDFExecutionEngine"},
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                },
                "class_name": "Datasource"
            }
        },
        "config_version": 1,
        "expectations_store_name": "expectations_store",
        "validation_results_store_name": "validation_results_store",
        "data_docs_store_name": "data_docs_store",
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {"class_name": "InMemoryStoreBackend"}
            },
            "validation_results_store": {
                "class_name": "ValidationResultsStore",
                "store_backend": {"class_name": "InMemoryStoreBackend"}
            },
            "data_docs_store": {
                "class_name": "DataDocsStore",
                "store_backend": {"class_name": "InMemoryStoreBackend"}
            }
        },
        "data_docs_sites": {
            "local_site": {
                "class_name": "SiteBuilder",
                "store_backend": {"class_name": "InMemoryStoreBackend"},
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"}
            }
        }
    }

    # 4️⃣ Create ephemeral GE context
    context = EphemeralDataContext(project_config=project_config)

    # 5️⃣ Create RuntimeBatchRequest
    batch_request = RuntimeBatchRequest(
        datasource_name="my_spark_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="spark_iceberg_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "run_20250710"}
    )

    # 6️⃣ Create expectation suite
    suite_name = "spark_iceberg_suite"
    context.create_expectation_suite(expectation_suite_name=suite_name, overwrite_existing=True)

    # 7️⃣ Get validator and define expectations
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    validator.expect_column_to_exist("flight_number")
    validator.expect_column_values_to_not_be_null("flight_date")
    validator.expect_column_values_to_be_between("distance_km", 0, 20000)

    results = validator.validate()

    # 8️⃣ Output result
    if not results.success:
        raise ValueError("❌ Validation failed.")
    else:
        print("✅ Validation passed.")

# Airflow Task
run_ge_check = PythonOperator(
    task_id='run_ge_check',
    python_callable=validate_iceberg_table_spark,
    dag=dag
)

run_ge_check
