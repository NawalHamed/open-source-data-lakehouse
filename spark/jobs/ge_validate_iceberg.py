from pyspark.sql import SparkSession
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations import get_context

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

        # Initialize Great Expectations context
        context = get_context()

        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="flight_data_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "flight_validation_1"}
        )

        # Create validator - let it create suite automatically
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="flight_data_expectations",
            create_expectation_suite_with_name_if_missing=True
        )

        # Define expectations
        validator.expect_column_values_to_not_be_null("flight_id")
        validator.expect_column_values_to_be_in_set(
            "status", 
            ["scheduled", "departed", "landed", "delayed", "cancelled"]
        )

        # Save the expectation suite (if using persistent context)
        try:
            validator.save_expectation_suite()
        except AttributeError:
            pass  # Skip for ephemeral context

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
