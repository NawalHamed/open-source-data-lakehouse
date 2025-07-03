from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType
from PIL import Image
import numpy as np
import io

# Step 1: SparkSession with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("Image Metadata + Decoded Data to Iceberg") \
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

# Step 2: Read images as binary
image_df = spark.read.format("binaryFile") \
    .load("s3a://lakehouse/bronze_layer/unstructured_images_raw_data/*.jpg") \
    .withColumn("file_name", regexp_replace(input_file_name(), ".*/", ""))

# Step 3: Collect and decode with Pillow
image_data_list = image_df.collect()

print(f"\nProcessing {len(image_data_list)} images:\n")

results = []
for row in image_data_list:
    file_path = row["path"]
    binary_data = row["content"]

    try:
        img = Image.open(io.BytesIO(binary_data))
        img_array = np.array(img)
        decode_status = f"SUCCESS - Shape: {img_array.shape}"
    except Exception as e:
        decode_status = f"ERROR decoding: {str(e)}"

    print(f"Image: {file_path}\nStatus: {decode_status}\n{'='*50}")
    results.append((file_path, decode_status))

# Step 4: Create Spark DataFrame without pandas
schema = StructType([
    StructField("file_path", StringType(), True),
    StructField("decode_status", StringType(), True)
])

final_df = spark.createDataFrame(results, schema=schema)


# Step 5: Write to Iceberg Table
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")


spark.sql("DROP TABLE IF EXISTS nessie.silver_layer.image_metadata_with_status")

final_df.writeTo("nessie.silver_layer.image_metadata_with_status").createOrReplace()

# Step 6: Validate
spark.read.table("nessie.silver_layer.image_metadata_with_status").show(truncate=False)
