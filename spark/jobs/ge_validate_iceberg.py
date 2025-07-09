from pyspark.sql import SparkSession
import great_expectations as ge

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
    
# Load Iceberg table
df = spark.read.format("iceberg").load("nessie.silver_layer.flight_data")

# Use Great Expectations directly on Spark DF
ge_df = ge.dataset.SparkDFDataset(df)

# Add expectations (you can add more)
ge_df.expect_column_values_to_not_be_null("flight_id")
ge_df.expect_column_values_to_be_in_set("status", ["on-time", "delayed", "cancelled"])

# Validate and print results
result = ge_df.validate()
print(result)