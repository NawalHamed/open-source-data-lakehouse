from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_replace
import pytesseract
from PIL import Image
import numpy as np
import io
import pandas as pd

# Step 1: SparkSession with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("Image Metadata + Decoded Text to Iceberg") \
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

# Step 2: Read images including binary data
image_df = spark.read.format("image") \
    .load("s3a://warehouse/bronze_layer/unstructured_images_raw_data/*.jpg") \
    .withColumn("file_name", regexp_replace(input_file_name(), ".*/", ""))

# Step 3: Collect to driver for decoding (safe for moderate datasets)
image_data_list = image_df.select(
    "image.origin",
    "image.height",
    "image.width",
    "image.nChannels",
    "image.mode",
    "image.data"
).collect()

print(f"\nDecoding {len(image_data_list)} images:\n")

# Step 4: Decode image, extract text, combine with metadata
results = []
for row in image_data_list:
    origin = row["origin"]
    height = row["height"]
    width = row["width"]
    channels = row["nChannels"]
    mode = row["mode"]
    binary_data = row["data"]

    try:
        img = Image.open(io.BytesIO(binary_data))
        extracted_text = pytesseract.image_to_string(img)
    except Exception as e:
        extracted_text = f"ERROR decoding: {str(e)}"

    print(f"Image: {origin}\nExtracted Text: {extracted_text}\n{'='*50}")

    results.append((origin, height, width, channels, mode, extracted_text))

# Step 5: Convert to Pandas, then Spark DataFrame
df_results = pd.DataFrame(results, columns=["file_path", "height", "width", "channels", "mode", "extracted_text"])
final_df = spark.createDataFrame(df_results)

# Step 6: Create namespace if not exists
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# Step 7: Write to Iceberg with Metadata + Decoded Text
final_df.writeTo("nessie.silver_layer.image_metadata_with_text").createOrReplace()

# Step 8: Validate - Show saved data
spark.read.table("nessie.silver_layer.image_metadata_with_text").show(truncate=False)
