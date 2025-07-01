from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_replace
from PIL import Image
import cv2
import numpy as np
import io
import pandas as pd

# Step 1: SparkSession with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("Image Metadata + Decoded Data to Iceberg") \
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

print(f"\nProcessing {len(image_data_list)} images:\n")

# Step 4: Decode image, validate, combine with metadata
results = []
for row in image_data_list:
    origin = row["origin"]
    height = row["height"]
    width = row["width"]
    channels = row["nChannels"]
    mode = row["mode"]
    binary_data = row["data"]

    try:
        # Decode binary to NumPy array
        np_array = np.frombuffer(binary_data, np.uint8)
        img_cv2 = cv2.imdecode(np_array, cv2.IMREAD_UNCHANGED)

        # Check if valid image
        if img_cv2 is not None:
            decoded_status = "SUCCESS - Image decoded"
        else:
            decoded_status = "ERROR - Invalid image data"

    except Exception as e:
        decoded_status = f"ERROR decoding: {str(e)}"

    print(f"Image: {origin}\nStatus: {decoded_status}\n{'='*50}")

    results.append((origin, height, width, channels, mode, decoded_status))

# Step 5: Convert to Pandas, t
