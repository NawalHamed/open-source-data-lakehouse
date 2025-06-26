from pyspark.sql import SparkSession

# Step 1: Create SparkSession (with URI fix to /api/v2)
spark = SparkSession.builder \
    .appName("Airline JSON to Iceberg Silver") \
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

# Step 2: Read JSON data from MinIO (semi-structured)
df = spark.read.option("multiline", "true").json("s3a://warehouse/bronze_layer/semi_structured_raw_data/airline_data/*.json")

# Step 3: Print schema and sample
df.printSchema()
df.show(5, truncate=False)

# Optional: Clean/Transform here for silver (rename columns, cast types, filter, etc.)
# df_cleaned = df.select(...)

# Step 4: Create namespace if needed
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# Step 5: Write to Iceberg Silver layer table
df.writeTo("nessie.silver_layer.airline_data").append()

# Step 6: Verify
spark.read.table("nessie.silver_layer.airline_data").show(5)
