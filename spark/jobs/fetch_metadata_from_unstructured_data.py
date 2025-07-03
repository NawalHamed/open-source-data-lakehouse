from pyspark.sql import SparkSession

# Step 1: Create SparkSession with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("Image Metadata to Iceberg via Nessie") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9009") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Step 2: Read image metadata from MinIO bucket
image_df = spark.read.format("image").load("s3a://lakehouse/bronze_layer/unstructured_images_raw_data/")

# Optional: Show schema and metadata
image_df.printSchema()
image_df.select(
    "image.origin",
    "image.height",
    "image.width",
    "image.nChannels",
    "image.mode"
).show(truncate=False)

# Step 3: Create namespace/schema in Iceberg via Nessie if it doesn't exist
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# Step 4: Write image metadata as an Iceberg table using Nessie catalog
#image_df.writeTo("nessie.silver_layer.image_metadata").append()
image_df.writeTo("nessie.silver_layer.image_metadata").createOrReplace()


# Step 5: Read back from the Iceberg table to verify
spark.read.table("nessie.silver_layer.image_metadata").limit(50).toPandas()
