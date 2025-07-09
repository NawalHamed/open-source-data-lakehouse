from pyspark.sql import SparkSession
from great_expectations import get_context
from great_expectations.core.batch import RuntimeBatchRequest

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


# 2️⃣ Read Iceberg table (Spark DataFrame)
df = spark.read.format("iceberg").load("nessie.silver_layer.flight_data")

# 3️⃣ Get GE context (automatically uses working_dir)
context = get_context()

# 4️⃣ Create expectation suite (if doesn't exist)
suite_name = "flight_data_expectations"
if not context.expectations.expectation_suite_exists(expectation_suite_name=suite_name):
    context.create_expectation_suite(expectation_suite_name=suite_name)

# 5️⃣ Create batch request with Spark DataFrame
batch_request = RuntimeBatchRequest(
    datasource_name="my_spark_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="flight_data",  # this is logical only, not a file
    runtime_parameters={"batch_data": df},
    batch_identifiers={"default_identifier_name": "iceberg_validation"},
)

# 6️⃣ Get Validator and define expectations
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

# Define expectations
validator.expect_column_values_to_not_be_null("flight_id")
validator.expect_column_values_to_be_in_set("status", ["on-time", "delayed", "cancelled"])

# 7️⃣ Save suite
validator.save_expectation_suite(discard_failed_expectations=False)

# 8️⃣ Run validation and print result
results = validator.validate()

# Optional: stop Spark session
spark.stop()

# Print result summary
success = results["success"]
print(f"\n✅ Data Validation {'PASSED' if success else 'FAILED'}")
exit(0 if success else 1)

if __name__ == "__main__":
    validation_results = validate_iceberg_data()
    print(validation_results)
