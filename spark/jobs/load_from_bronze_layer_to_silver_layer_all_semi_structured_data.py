from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, initcap, col
from datetime import datetime

# 1️⃣ Dynamic Date Detection for Flights
now = datetime.utcnow()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
bronze_flight_path = f"s3a://lakehouse/bronze_layer/{year}/{month}/{day}/json/flight_data/*.json"

# 2️⃣ Paths for Master Data
bronze_airline_path = "s3a://lakehouse/bronze_layer/master/airlines_data.json"
bronze_airport_path = "s3a://lakehouse/bronze_layer/master/airports_data.json"

# 3️⃣ Initialize Spark
spark = SparkSession.builder \
    .appName("Bronze to Silver Incremental Load") \
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


#spark.sql("DROP TABLE IF EXISTS nessie.silver_layer.airline_data")
#spark.sql("DROP TABLE IF EXISTS nessie.silver_layer.airport_data")
#spark.sql("DROP TABLE IF EXISTS nessie.silver_layer.flight_data")

# 4️⃣ Load Bronze Data
df_airline = spark.read.option("multiline", "true").json(bronze_airline_path)
df_airport = spark.read.option("multiline", "true").json(bronze_airport_path)
df_flight = spark.read.option("multiline", "true").json(bronze_flight_path)

# 5️⃣ Clean and Transform
df_airline_clean = df_airline.na.fill({"name": "UNKNOWN", "country": "UNKNOWN", "iata": "XXX"}) \
    .withColumn("name", initcap(trim(col("name")))) \
    .withColumn("country", upper(trim(col("country")))) \
    .withColumn("iata", upper(trim(col("iata")))) \
    .dropDuplicates()

df_airport_clean = df_airport.na.fill({"country_name": "UNKNOWN", "iata_code": "XXX"}) \
    .withColumn("country_name", upper(trim(col("country_name")))) \
    .withColumn("iata_code", upper(trim(col("iata_code")))) \
    .dropDuplicates()

df_flight_clean = df_flight.na.fill({"status": "UNKNOWN"}) \
    .withColumn("flight_number", upper(trim(col("flight_number")))) \
    .dropDuplicates()

# 6️⃣ Ensure Namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")



# 7️⃣ Airline MERGE using DataFrame join logic
try:
    df_existing_airline = spark.read.format("iceberg").load("nessie.silver_layer.airline_data")
    df_merged_airline = df_existing_airline.alias("target").join(
        df_airline_clean.alias("source"), "id", "outer"
    ).selectExpr(
        "coalesce(source.id, target.id) as id",
        "coalesce(source.name, target.name) as name",
        "coalesce(source.country, target.country) as country",
        "coalesce(source.iata, target.iata) as iata",
        "coalesce(source.icao, target.icao) as icao",
        "coalesce(source.callsign, target.callsign) as callsign",
        "coalesce(source.country_code, target.country_code) as country_code",
        "coalesce(source.hub, target.hub) as hub",
        "coalesce(source.status, target.status) as status",
        "coalesce(source.created_at, target.created_at) as created_at",
        "coalesce(source.updated_at, target.updated_at) as updated_at"
    )
    df_merged_airline.writeTo("nessie.silver_layer.airline_data").overwritePartitions()
except:
    df_airline_clean.writeTo("nessie.silver_layer.airline_data").createOrReplace()

# 8️⃣ Airport MERGE using DataFrame join logic
try:
    df_existing_airport = spark.read.format("iceberg").load("nessie.silver_layer.airport_data")
    df_merged_airport = df_existing_airport.alias("target").join(
        df_airport_clean.alias("source"), "id", "outer"
    ).selectExpr(
        "coalesce(source.id, target.id) as id",
        "coalesce(source.airport_id, target.airport_id) as airport_id",
        "coalesce(source.iata_code, target.iata_code) as iata_code",
        "coalesce(source.icao_code, target.icao_code) as icao_code",
        "coalesce(source.country_iso2, target.country_iso2) as country_iso2",
        "coalesce(source.country_name, target.country_name) as country_name",
        "coalesce(source.airport_name, target.airport_name) as airport_name",
        "coalesce(source.created_at, target.created_at) as created_at",
        "coalesce(source.updated_at, target.updated_at) as updated_at"
    )
    df_merged_airport.writeTo("nessie.silver_layer.airport_data").overwritePartitions()
except:
    df_airport_clean.writeTo("nessie.silver_layer.airport_data").createOrReplace()

# 9️⃣ Flights are Daily — Append Only
try:
    df_flight_clean.writeTo("nessie.silver_layer.flight_data").append()
except:
    df_flight_clean.writeTo("nessie.silver_layer.flight_data").createOrReplace()



# 🔟 Optional: Verify
print("✅ Airlines Table Preview:")
spark.read.table("nessie.silver_layer.airline_data").show(5)

print("✅ Airports Table Preview:")
spark.read.table("nessie.silver_layer.airport_data").show(5)

print("✅ Flights Table Preview:")
spark.read.table("nessie.silver_layer.flight_data").show(5)

spark.stop()



