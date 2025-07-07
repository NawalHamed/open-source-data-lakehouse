from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, initcap, col
from datetime import datetime

# 1️⃣ Spark Session Setup
spark = SparkSession.builder \
    .appName("Bronze to Silver Lakehouse with Detection Logic") \
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

# 2️⃣ Date Detection for Daily Flight Partition
now = datetime.utcnow()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
bronze_flight_path = f"s3a://lakehouse/bronze_layer/{year}/{month}/{day}/json/flight_data/*.json"

# 3️⃣ Load Data
df_airline = spark.read.option("multiline", "true").json("s3a://lakehouse/bronze_layer/master/airlines_data.json")
df_airport = spark.read.option("multiline", "true").json("s3a://lakehouse/bronze_layer/master/airports_data.json")
df_flight = spark.read.option("multiline", "true").json(bronze_flight_path)

# 4️⃣ Cleaning & Transformations
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

# 5️⃣ Create Namespace if Needed
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# 6️⃣ Detect if Tables Exist
table_list = [row.tableName for row in spark.sql("SHOW TABLES IN nessie.silver_layer").collect()]

# 7️⃣ Airlines Logic
if "airline_data" in table_list:
    print("✅ Airline table exists - MERGE INTO")
    df_airline_clean.createOrReplaceTempView("airline_updates")
    spark.sql("""
    MERGE INTO nessie.silver_layer.airline_data AS target
    USING airline_updates AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
else:
    print("🆕 Airline table does not exist - creating new table")
    df_airline_clean.writeTo("nessie.silver_layer.airline_data").createOrReplace()

# 8️⃣ Airports Logic
if "airport_data" in table_list:
    print("✅ Airport table exists - MERGE INTO")
    df_airport_clean.createOrReplaceTempView("airport_updates")
    spark.sql("""
    MERGE INTO nessie.silver_layer.airport_data AS target
    USING airport_updates AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
else:
    print("🆕 Airport table does not exist - creating new table")
    df_airport_clean.writeTo("nessie.silver_layer.airport_data").createOrReplace()

# 9️⃣ Flights Logic (Always Append)
if "flight_data" in table_list:
    print("✈ Appending new flight records")
    df_flight_clean.writeTo("nessie.silver_layer.flight_data").append()
else:
    print("🆕 Flight table does not exist - creating new table")
    df_flight_clean.writeTo("nessie.silver_layer.flight_data").createOrReplace()

# 🔟 Optional: Verify
print("✅ Airlines Table Preview:")
spark.read.table("nessie.silver_layer.airline_data").show(5)

print("✅ Airports Table Preview:")
spark.read.table("nessie.silver_layer.airport_data").show(5)

print("✅ Flights Table Preview:")
spark.read.table("nessie.silver_layer.flight_data").show(5)
