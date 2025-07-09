from pyspark.sql import SparkSession
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.expectation_suite import ExpectationSuite

def validate_iceberg_data():
    # Initialize Spark with Iceberg configuration
    spark = SparkSession.builder \
        .appName("GE Iceberg Validation") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9009") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    try:
        # Load Iceberg table
        df = spark.table("nessie.silver_layer.flight_data")

        # Initialize Ephemeral DataContext
        context = EphemeralDataContext(project_config={
            "config_variables_file_path": None,
            "datasources": {},
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "InMemoryStoreBackend"
                    }
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "InMemoryStoreBackend"
                    }
                }
            },
            "expectations_store_name": "expectations_store",
            "validations_store_name": "validations_store",
            "data_docs_sites": {},
            "anonymous_usage_statistics": {
                "enabled": False
            }
        })

        # Create expectation suite
        suite = ExpectationSuite(
            expectation_suite_name="flight_data_expectations"
        )

        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="flight_data_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "flight_validation_1"}
        )

        # Create validator
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )

        # Define expectations
        validator.expect_column_values_to_not_be_null("flight_id")
        validator.expect_column_values_to_be_in_set(
            "status", 
            ["scheduled", "departed", "landed", "delayed", "cancelled"]
        )

        # Run validation
        results = validator.validate()
        
        return {
            "success": results.success,
            "statistics": results.statistics,
            "results": [str(result) for result in results.results]
        }
        
    finally:
        spark.stop()

if __name__ == "__main__":
    validation_results = validate_iceberg_data()
    print(validation_results)
