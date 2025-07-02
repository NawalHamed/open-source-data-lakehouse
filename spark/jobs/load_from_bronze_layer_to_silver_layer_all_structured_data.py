from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, initcap, col

# Step 1: Spark Session with Iceberg + Nessie + MinIO
spark = SparkSession.builder \
    .appName("All Structured Data to Iceberg Silver with Cleaning") \
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

# ----------- WEATHER DATA ---------------
print("Loading Weather Data...")
weather_df = spark.read.option("header", True).option("inferSchema", True).csv("s3a://lakehouse/bronze_layer/structured_raw_data/weather_data/*.csv")

# Cleaning
weather_df_clean = weather_df \
    .na.fill({"weather_desc": "UNKNOWN", "city": "UNKNOWN"}) \
    .withColumn("weather_desc", upper(trim(col("weather_desc")))) \
    .withColumn("city", initcap(trim(col("city")))) \
    .dropDuplicates()

weather_df_clean.printSchema()
weather_df_clean.show(5)

print("Writing Weather Data to Iceberg...")
weather_df_clean.writeTo("nessie.silver_layer.weather_data").createOrReplace()
#weather_df_clean.writeTo("nessie.silver_layer.weather_data").append()

# ----------- COUNTRIES DATA ---------------
print("Loading Countries Data...")
countries_df = spark.read.option("header", True).option("inferSchema", True).csv("s3a://lakehouse/bronze_layer/structured_raw_data/countries_data/*.csv")

# Cleaning
countries_df_clean = countries_df \
    .na.fill({"name": "UNKNOWN", "iso2": "XXX"}) \
    .withColumn("name", initcap(trim(col("name")))) \
    .withColumn("iso2", upper(trim(col("iso2")))) \
    .dropDuplicates()

countries_df_clean.printSchema()
countries_df_clean.show(5)

print("Writing Countries Data to Iceberg...")
countries_df_clean.writeTo("nessie.silver_layer.countries_data").createOrReplace()
#countries_df_clean.writeTo("nessie.silver_layer.countries_data").append()

# ----------- CITIES DATA ---------------
print("Loading Cities Data...")
cities_df = spark.read.option("header", True).option("inferSchema", True).csv("s3a://lakehouse/bronze_layer/structured_raw_data/cities_data/*.csv")

# Cleaning
cities_df_clean = cities_df \
    .na.fill({"city_name": "UNKNOWN", "country_iso2": "UNKNOWN"}) \
    .withColumn("city_name", initcap(trim(col("city_name")))) \
    .dropDuplicates()

cities_df_clean.printSchema()
cities_df_clean.show(5)

print("Writing Cities Data to Iceberg...")
cities_df_clean.writeTo("nessie.silver_layer.cities_data").createOrReplace()
#cities_df_clean.writeTo("nessie.silver_layer.cities_data").append()

# Step 4: Verify
print("\nSample from Silver Layer Tables:")

print("\nWeather Data:")
spark.read.table("nessie.silver_layer.weather_data").show(5)

print("\nCountries Data:")
spark.read.table("nessie.silver_layer.countries_data").show(5)

print("\nCities Data:")
spark.read.table("nessie.silver_layer.cities_data").show(5)

print("All structured datasets cleaned and written to Iceberg Silver Layer!")
