from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, initcap, col
from pyspark.sql.utils import AnalysisException

# Step 1: Spark Session Setup with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("Bronze to Iceberg Silver - All Datasets") \
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

# Step 2: Read JSON data dynamically (latest path)
year = spark.sql("SELECT year(current_date())").collect()[0][0]
month = f"{spark.sql('SELECT month(current_date())').collect()[0][0]:02d}"
day = f"{spark.sql('SELECT day(current_date())').collect()[0][0]:02d}"

bronze_path = f"s3a://lakehouse/bronze_layer/{year}/{month}/{day}/json"

df_airline = spark.read.option("multiline", "true").json(f"{bronze_path}/airlines_data/*.json")
df_airport = spark.read.option("multiline", "true").json(f"{bronze_path}/airport_data/*.json")
df_flight = spark.read.option("multiline", "true").json(f"{bronze_path}/flight_data/*.json")

# Step 3: Cleaning and Transformation
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

# Step 4: Create namespace if needed
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# Step 5: Airlines Merge or Initial Load
try:
    df_existing = spark.read.table("nessie.silver_layer.airline_data")
    print("✅ Airline table exists - Merging DataFrame")

    df_merged = df_existing.alias("old").join(
        df_airline_clean.alias("new"), on="id", how="outer"
    ).selectExpr(
        "coalesce(new.id, old.id) as id",
        "coalesce(new.name, old.name) as name",
        "coalesce(new.iata, old.iata) as iata",
        "coalesce(new.icao, old.icao) as icao",
        "coalesce(new.callsign, old.callsign) as callsign",
        "coalesce(new.country_code, old.country_code) as country_code",
        "coalesce(new.country, old.country) as country",
        "coalesce(new.hub, old.hub) as hub",
        "coalesce(new.status, old.status) as status",
        "coalesce(new.created_at, old.created_at) as created_at",
        "coalesce(new.updated_at, old.updated_at) as updated_at"
    )
    df_merged.writeTo("nessie.silver_layer.airline_data").overwritePartitions()
except AnalysisException:
    print("✅ Airline table does not exist - Creating new")
    df_airline_clean.writeTo("nessie.silver_layer.airline_data").createOrReplace()

# Step 6: Airports Merge or Initial Load
try:
    df_existing_airport = spark.read.table("nessie.silver_layer.airport_data")
    print("✅ Airport table exists - Merging DataFrame")

    df_merged_airport = df_existing_airport.alias("old").join(
        df_airport_clean.alias("new"), on="id", how="outer"
    ).selectExpr(
        "coalesce(new.id, old.id) as id",
        "coalesce(new.airport_id, old.airport_id) as airport_id",
        "coalesce(new.iata_code, old.iata_code) as iata_code",
        "coalesce(new.icao_code, old.icao_code) as icao_code",
        "coalesce(new.country_iso2, old.country_iso2) as country_iso2",
        "coalesce(new.country_name, old.country_name) as country_name",
        "coalesce(new.airport_name, old.airport_name) as airport_name",
        "coalesce(new.created_at, old.created_at) as created_at",
        "coalesce(new.updated_at, old.updated_at) as updated_at"
    )
    df_merged_airport.writeTo("nessie.silver_layer.airport_data").overwritePartitions()
except AnalysisException:
    print("✅ Airport table does not exist - Creating new")
    df_airport_clean.writeTo("nessie.silver_layer.airport_data").createOrReplace()

# Step 7: Flights Append (Daily)
df_flight_clean.writeTo("nessie.silver_layer.flight_data").append()

# Step 8: Verification
print("=== Airlines Preview ===")
spark.read.table("nessie.silver_layer.airline_data").show(5)
print("=== Airports Preview ===")
spark.read.table("nessie.silver_layer.airport_data").show(5)
print("=== Flights Preview ===")
spark.read.table("nessie.silver_layer.flight_data").show(5)

print("✅ Bronze to Silver Process Completed")
