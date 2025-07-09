from pyspark.sql import SparkSession
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext

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

    # Load Iceberg table
    df = spark.table("nessie.silver_layer.flight_data")

    # Initialize Great Expectations context
    context = DataContext()

    # Create batch request
    batch_request = RuntimeBatchRequest(
        datasource_name="default_spark_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="flight_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": "flight_validation_1"}
    )

    # Create validator
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

    # Run validation
    results = validator.validate()

    # Stop Spark session
    spark.stop()

    return results

if __name__ == "__main__":
    validation_results = validate_iceberg_data()
    print(validation_results)
