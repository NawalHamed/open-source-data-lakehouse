from pyspark.sql import SparkSession
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.validator.validator import Validator
from great_expectations.data_context import BaseDataContext

# ================= Spark Setup =================
spark = SparkSession.builder \
    .appName("GE Iceberg Validation") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9009") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()
    
# Step 2: Load Iceberg table into Spark DataFrame
df = spark.read.format("iceberg").load("nessie.silver_layer.flight_data")

# Step 3: Create GE context
context = BaseDataContext()

# Step 4: Create RuntimeBatchRequest
batch_request = RuntimeBatchRequest(
    datasource_name="my_spark_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="flight_data",
    runtime_parameters={"batch_data": df},
    batch_identifiers={"default_identifier_name": "iceberg_validation"},
)

# Step 5: Create Validator (no suite needed for programmatic expectations)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="tmp_flight_suite"
)
validator.expect_column_values_to_not_be_null("flight_id")
validator.expect_column_values_to_be_in_set("status", ["on-time", "delayed", "cancelled"])

# Step 6: Validate
results = validator.validate()
print(results)
