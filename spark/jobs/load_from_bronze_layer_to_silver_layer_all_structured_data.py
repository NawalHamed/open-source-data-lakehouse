from pyspark.sql import SparkSession

# Step 1: Create SparkSession (with corrected Nessie URI if needed)
spark = SparkSession.builder \
    .appName("All Structured Data to Iceberg Silver") \
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

# Step 2: Create namespace if needed
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# Step 3: Load, inspect, and write each dataset

# ----------- WEATHER DATA ---------------
print("Loading Weather Data...")
weather_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("s3a://warehouse/bronze_layer/structured_raw_data/weather_data/*.csv")

weather_df.printSchema()
weather_df.show(5)

print(" Writing Weather Data to Iceberg...")
weather_df.writeTo("nessie.silver_layer.weather_data").createOrReplace()

# ----------- COUNTRIES DATA ---------------
print(" Loading Countries Data...")
countries_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("s3a://warehouse/bronze_layer/structured_raw_data/countries_data/*.csv")

countries_df.printSchema()
countries_df.show(5)

print(" Writing Countries Data to Iceberg...")
countries_df.writeTo("nessie.silver_layer.countries_data").createOrReplace()

# ----------- CITIES DATA ---------------
print(" Loading Cities Data...")
cities_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("s3a://warehouse/bronze_layer/structured_raw_data/cities_data/*.csv")

cities_df.printSchema()
cities_df.show(5)

print(" Writing Cities Data to Iceberg...")
cities_df.writeTo("nessie.silver_layer.cities_data").createOrReplace()

# Step 4: Verify
print(" Sample from Silver Layer Tables:")

print("\nWeather Data:")
spark.read.table("nessie.silver_layer.weather_data").show(5)

print("\nCountries Data:")
spark.read.table("nessie.silver_layer.countries_data").show(5)

print("\nCities Data:")
spark.read.table("nessie.silver_layer.cities_data").show(5)

print(" All datasets successfully loaded to Iceberg Silver Layer!")
