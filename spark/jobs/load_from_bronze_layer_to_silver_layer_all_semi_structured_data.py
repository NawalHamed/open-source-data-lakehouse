from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, initcap, col
from datetime import datetime

# 1️⃣ Spark Session Setup
spark = SparkSession.builder \
    .appName("Bronze to Iceberg Silver - Full Reset") \
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

# 2️⃣ Dynamic Date for Flight Bronze Layer
now = datetime.utcnow()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
bronze_flight_path = f"s3a://lakehouse/bronze_layer/{year}/{month}/{day}/json/flight_data/*.json"

# 3️⃣ Drop Tables if Exist
spark.sql("DROP TABLE IF EXISTS nessie.silver_layer.airline_data")
spark.sql("DROP TABLE IF EXISTS nessie.silver_layer.airport_data")
spark.sql("DROP TABLE IF EXISTS nessie.silver_layer.flight_data")

# 4️⃣ Load Data
df_airline = spark.read.option("multiline", "true").json("s3a://lakehouse/bronze_layer/master/airlines_data.json")
df_airport = spark.read.option("multiline", "true").json("s3a://lakehouse/bronze_layer/master/airports_data.json")
df_flight = spark.read.option("multiline", "true").json(bronze_flight_path)

# 5️⃣ Cleaning & Transformations
df_airline_clean = df_airline \
    .withColumn("name", initcap(trim(col("name")))) \
    .withColumn("country", upper(trim(col("country")))) \
    .withColumn("iata", upper(trim(col("iata")))) \
    .dropDuplicates(["id"])

df_airport_clean = df_airport \
    .withColumn("country_name", upper(trim(col("country_name")))) \
    .withColumn("iata_code", upper(trim(col("iata_code")))) \
    .dropDuplicates(["id"])

df_flight_clean = df_flight \
    .withColumn("flight_number", upper(trim(col("flight_number")))) \
    .dropDuplicates(["flight_id"])

# 6️⃣ Create Namespace if Needed
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# 7️⃣ Re-Create Clean Tables
df_airline_clean.writeTo("nessie.silver_layer.airline_data").createOrReplace()
df_airport_clean.writeTo("nessie.silver_layer.airport_data").createOrReplace()
df_flight_clean.writeTo("nessie.silver_layer.flight_data").createOrReplace()

# 8️⃣ Verify Results
print("✅ Airlines Recreated:")
spark.read.table("nessie.silver_layer.airline_data").show(5)

print("✅ Airports Recreated:")
spark.read.table("nessie.silver_layer.airport_data").show(5)

print("✅ Flights Recreated:")
spark.read.table("nessie.silver_layer.flight_data").show(5)
