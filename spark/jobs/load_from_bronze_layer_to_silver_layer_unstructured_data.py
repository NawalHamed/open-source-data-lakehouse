from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_replace, pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from PIL import Image
import numpy as np
import pytesseract
import io
import pandas as pd

# Step 1: SparkSession with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("Image Metadata + Decoded Data + OCR Text to Iceberg") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "123hhbj211hjb1464") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9009") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Step 2: Define the schema for the output DataFrame
schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("decode_status", StringType(), True),
    StructField("height", IntegerType(), True),
    StructField("width", IntegerType(), True),
    StructField("n_channels", IntegerType(), True),
    StructField("mode", StringType(), True),
    StructField("pixel_data", StringType(), True),
    StructField("ocr_text", StringType(), True),
])

# Step 3: Define the Pandas UDF for image processing
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def process_images_udf(df: pd.DataFrame) -> pd.DataFrame:
    results = []
    for index, row in df.iterrows():
        file_name = row["file_name"]
        binary_data = row["content"]

        try:
            img = Image.open(io.BytesIO(binary_data))
            img_array = np.array(img)
            height, width = img.height, img.width
            mode = img.mode
            n_channels = img_array.shape[2] if len(img_array.shape) == 3 else 1
            pixel_sample = img_array[:2, :2].tolist()
            pixel_data_str = str(pixel_sample)
            ocr_text = pytesseract.image_to_string(img)
            decode_status = "SUCCESS"
        except Exception as e:
            height, width, n_channels, mode = None, None, None, None
            pixel_data_str = ""
            ocr_text = ""
            decode_status = f"ERROR: {str(e)}"
        
        results.append((file_name, decode_status, height, width, n_channels, mode, pixel_data_str, ocr_text))
    
    return pd.DataFrame(results, columns=schema.names)

# Step 4: Read images and apply the UDF
image_df = spark.read.format("binaryFile") \
    .load("s3a://lakehouse/bronze_layer/*/*/*/images/*.jpg") \
    .withColumn("file_name", regexp_replace(input_file_name(), ".*/", ""))

# Add a grouping column for the UDF to ensure each row is processed individually
processed_df = image_df.withColumn("group_id", input_file_name()).groupBy("group_id").apply(process_images_udf)

# Drop the temporary grouping column
final_df = processed_df.drop("group_id")

# Step 5: Write to Iceberg table
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")
final_df.writeTo("nessie.silver_layer.image_metadata_with_text").createOrReplace()

# Step 6: Show result
spark.read.table("nessie.silver_layer.image_metadata_with_text").show(truncate=False)

spark.stop()
