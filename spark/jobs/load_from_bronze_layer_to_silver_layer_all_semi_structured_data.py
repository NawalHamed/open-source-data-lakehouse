from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, initcap, col, to_timestamp

# Step 1: Spark Session with Iceberg + Nessie + MinIO
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

# Step 2: Read JSON data from MinIO
df_airline = spark.read.option("multiline", "true").json("s3a://lakehouse/bronze_layer/semi_structured_raw_data/airline_data/*.json")
df_airport = spark.read.option("multiline", "true").json("s3a://lakehouse/bronze_layer/semi_structured_raw_data/airport_data/*.json")
df_flight = spark.read.option("multiline", "true").json("s3a://lakehouse/bronze_layer/semi_structured_raw_data/flight_data/*.json")

# Step 3: Cleaning and Transformation

# Airline Dataset
df_airline_clean = df_airline \
    .na.fill({"name": "UNKNOWN", "country": "UNKNOWN", "iata": "XXX"}) \
    .withColumn("name", initcap(trim(col("name")))) \
    .withColumn("country", upper(trim(col("country")))) \
    .withColumn("iata", upper(trim(col("iata")))) \
    .dropDuplicates()

# Airport Dataset
df_airport_clean = df_airport \
    .na.fill({"country_name": "UNKNOWN", "iata_code": "XXX"}) \
    .withColumn("country_name", upper(trim(col("country_name")))) \
    .withColumn("iata_code", upper(trim(col("iata_code")))) \
    .dropDuplicates()

# Flight Dataset
df_flight_clean = df_flight \
    .na.fill({"status": "UNKNOWN"}) \
    .withColumn("flight_number", upper(trim(col("flight_number")))) \
    .dropDuplicates()

# Step 4: Create namespace if needed
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# Step 5: Write to Iceberg Silver layer tables

df_airline_clean.writeTo("nessie.silver_layer.airline_data").createOrReplace()
df_airport_clean.writeTo("nessie.silver_layer.airport_data").createOrReplace()
df_flight_clean.writeTo("nessie.silver_layer.flight_data").createOrReplace()


#df_airline_clean.writeTo("nessie.silver_layer.airline_data").append()
#df_airport_clean.writeTo("nessie.silver_layer.airport_data").append()
#df_flight_clean.writeTo("nessie.silver_layer.flight_data").append()



# Step 6: Verify Silver Tables
print("=== Verify Airline Silver ===")
spark.read.table("nessie.silver_layer.airline_data").show(5)

print("=== Verify Airport Silver ===")
spark.read.table("nessie.silver_layer.airport_data").show(5)

print("=== Verify Flight Silver ===")
spark.read.table("nessie.silver_layer.flight_data").show(5)
