from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from PIL import Image
import numpy as np
import pytesseract
import io
import json

# Step 1: Create SparkSession with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("Extract Table Metadata + OCR JSON from Images") \
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

# Step 2: Load Images from MinIO
image_df = spark.read.format("binaryFile") \
    .load("s3a://lakehouse/bronze_layer/*/*/*/images/*.jpg") \
    .withColumn("file_name", regexp_replace(input_file_name(), ".*/", ""))

# Step 3: Decode and OCR (Parse Table as JSON)
image_data_list = image_df.collect()
results = []

def extract_table_as_json(image: Image.Image):
    raw_text = pytesseract.image_to_string(image, config='--oem 3 --psm 6')
    lines = [line.strip() for line in raw_text.split('\n') if line.strip()]

    headers = ["Flight", "Airline", "From", "To", "Aircraft", "Distance", "Status"]
    table = []

    for line in lines:
        parts = line.split()
        if len(parts) >= 7:
            try:
                flight = parts[0]
                airline = parts[1]
                from_airport = parts[2]
                to_airport = parts[3]
                aircraft = " ".join(parts[4:-2])
                distance = parts[-2].replace("km", "")
                status = parts[-1]

                table.append({
                    "Flight": flight,
                    "Airline": airline,
                    "From": from_airport,
                    "To": to_airport,
                    "Aircraft": aircraft,
                    "Distance": distance,
                    "Status": status
                })
            except Exception:
                continue
    return json.dumps(table)

for row in image_data_list:
    file_name = row["file_name"]
    binary_data = row["content"]

    try:
        img = Image.open(io.BytesIO(binary_data))
        img_array = np.array(img)
        height, width = img.height, img.width
        n_channels = img_array.shape[2] if len(img_array.shape) == 3 else 1
        ocr_text = extract_table_as_json(img)
        decode_status = "SUCCESS"
    except Exception as e:
        height, width, n_channels = None, None, None
        ocr_text = ""
        decode_status = f"ERROR: {str(e)}"

    results.append((file_name, decode_status, height, width, n_channels, ocr_text))

# Step 4: Convert to DataFrame
schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("decode_status", StringType(), True),
    StructField("height", IntegerType(), True),
    StructField("width", IntegerType(), True),
    StructField("n_channels", IntegerType(), True),
    StructField("ocr_text", StringType(), True)
])

final_df = spark.createDataFrame(results, schema=schema)

# Step 5: Write to Iceberg Table
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")
spark.sql("DROP TABLE IF EXISTS nessie.silver_layer.flight_table_ocr_json")

final_df.writeTo("nessie.silver_layer.flight_table_ocr_json").createOrReplace()

# Step 6: View Output
final_df.show(truncate=False)
