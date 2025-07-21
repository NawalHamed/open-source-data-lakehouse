from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, initcap, col

# 🔹 File paths in MinIO
csv_file_path = "s3a://lakehouse/bronze_layer/2025/07/21/csv/weather_data/weather_data_000003.csv"
json_file_path = "s3a://lakehouse/bronze_layer/2025/07/21/json/flight_data/flight_data_000004.json"

# 🔹 PostgreSQL config
pg_url = "jdbc:postgresql://postgres:5432/airflow"
pg_user = "airflow"
pg_password = "airflow"
pg_driver = "org.postgresql.Driver"

# 1️⃣ Initialize SparkSession (JAR already in image, no need to add .config("spark.jars"))
spark = SparkSession.builder \
    .appName("Bronze to PostgreSQL Load") \
    .master("spark://spark-master:7077") \
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

# 2️⃣ Read and clean CSV
df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file_path)
#df_csv_clean = df_csv.dropDuplicates().na.fill("UNKNOWN")
#df_csv_clean = df_csv_clean.select([
#    initcap(trim(col(c))).alias(c) if df_csv_clean.schema[c].dataType.simpleString() == 'string' else col(c)
#    for c in df_csv_clean.columns
#])

# 3️⃣ Read and clean JSON
df_json = spark.read.option("multiline", "true").json(json_file_path)
#df_json_clean = df_json.dropDuplicates().na.fill("UNKNOWN")
#df_json_clean = df_json_clean.select([
#    initcap(trim(col(c))).alias(c) if df_json_clean.schema[c].dataType.simpleString() == 'string' else col(c)
#    for c in df_json_clean.columns
#])

# 4️⃣ Write weather_data to PostgreSQL
df_csv_clean.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "public.weather_data_csv") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()

# 5️⃣ Write flight_data to PostgreSQL
df_json_clean.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "public.flight_data_json") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()

# ✅ Optional Preview
print("✅ Weather Data Preview:")
df_csv_clean.show(5)

print("✅ Flight Data Preview:")
df_json_clean.show(5)

spark.stop()
