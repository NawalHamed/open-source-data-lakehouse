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
# Step 2: Load CSV from MinIO
csv_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("s3a://warehouse/bronze_layer/structured_raw_data/weather-data/*.csv")

# Optional: inspect schema
csv_df.printSchema()
csv_df.show(5)

# Step 3: Create namespace if needed
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")


# Step 4: Write to Iceberg table
#csv_df.writeTo("nessie.silver_layer.weather_data").createOrReplace()

csv_df.writeTo("nessie.silver_layer.weather_data").createOrReplace()


# Step 5: Verify
spark.read.table("nessie.silver_layer.weather_data").show(5)
