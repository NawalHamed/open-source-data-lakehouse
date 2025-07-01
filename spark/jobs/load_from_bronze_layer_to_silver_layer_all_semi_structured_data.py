from pyspark.sql import SparkSession

# Step 1: Create SparkSession with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("Bronze to Iceberg Silver - All Datasets") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9009") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Step 2: Read JSON data from MinIO (semi-structured)
df_airline = spark.read.option("multiline", "true").json("s3a://warehouse/bronze_layer/semi_structured_raw_data/airline_data/*.json")
df_airport = spark.read.option("multiline", "true").json("s3a://warehouse/bronze_layer/semi_structured_raw_data/airport_data/*.json")
df_flight = spark.read.option("multiline", "true").json("s3a://warehouse/bronze_layer/semi_structured_raw_data/flight_data/*.json")

# Step 3: Print schema and sample for each
print("=== Airline Data ===")
df_airline.printSchema()
df_airline.show(5, truncate=False)

print("=== Airport Data ===")
df_airport.printSchema()
df_airport.show(5, truncate=False)

print("=== Flight Data ===")
df_flight.printSchema()
df_flight.show(5, truncate=False)

# Step 4: Create namespace if needed
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# Step 5: Write to Iceberg Silver layer tables
df_airline.writeTo("nessie.silver_layer.airline_data").createOrReplace()
df_airport.writeTo("nessie.silver_layer.airport_data").createOrReplace()
df_flight.writeTo("nessie.silver_layer.flight_data").createOrReplace()

#df_airline.writeTo("nessie.silver_layer.airline_data").append()
#df_airport.writeTo("nessie.silver_layer.airport_data").append()
#df_flight.writeTo("nessie.silver_layer.flight_data").append()

# Step 6: Verify Silver Tables
print("=== Verify Airline Silver ===")
spark.read.table("nessie.silver_layer.airline_data").show(5)

print("=== Verify Airport Silver ===")
spark.read.table("nessie.silver_layer.airport_data").show(5)

print("=== Verify Flight Silver ===")
spark.read.table("nessie.silver_layer.flight_data").show(5)

