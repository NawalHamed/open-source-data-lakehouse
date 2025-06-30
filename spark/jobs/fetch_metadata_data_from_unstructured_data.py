from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit, regexp_replace

# Step 1: SparkSession with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("Image Metadata and Text Data to Iceberg") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9009") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Step 2: Read image metadata
image_df = spark.read.format("image") \
    .load("s3a://warehouse/bronze_layer/unstructured_images_raw_data/*.jpg") \
    .withColumn("file_name", regexp_replace(input_file_name(), ".*/", "")) \
    .withColumn("text_file", regexp_replace(input_file_name(), ".jpg", ".txt"))

# Step 3: Read corresponding text files
text_df = spark.read.text("s3a://warehouse/bronze_layer/unstructured_images_raw_data/*.txt") \
    .withColumn("text_file", regexp_replace(input_file_name(), ".*/", ""))

# Step 4: Join image metadata with text data
joined_df = image_df.join(
    text_df,
    image_df.text_file == text_df.text_file,
    how="left"
).select(
    image_df["image.origin"].alias("file_path"),
    image_df["image.height"].alias("height"),
    image_df["image.width"].alias("width"),
    image_df["image.nChannels"].alias("channels"),
    image_df["image.mode"].alias("mode"),
    text_df["value"].alias("extracted_text")
)

# Step 5: Create namespace if not exists
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# Step 6: Write combined data to Iceberg
joined_df.writeTo("nessie.silver_layer.image_with_text").createOrReplace()

# Step 7: Print combined data
result_df = spark.read.table("nessie.silver_layer.image_with_text")
result_df.show(truncate=False)
