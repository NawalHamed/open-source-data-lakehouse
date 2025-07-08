from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, initcap, col, coalesce
from datetime import datetime

# 1️⃣ Dynamic Date Detection for Weather Data
now = datetime.utcnow()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
bronze_weather_path = f"s3a://lakehouse/bronze_layer/{year}/{month}/{day}/csv/weather_data/*.csv"

# 2️⃣ Paths for Master Data
bronze_country_path = "s3a://lakehouse/bronze_layer/master/countries_data.csv"
bronze_city_path = "s3a://lakehouse/bronze_layer/master/cities_data.csv"

# 3️⃣ Initialize Spark
spark = SparkSession.builder \
    .appName("Structured Data Bronze to Silver Incremental Load") \
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

spark.catalog.dropTable("nessie.gold_layer.airport_capacity_analysis", purge=True)
spark.catalog.dropTable("nessie.gold_layer.flight_performance_summary", purge=True)


# 4️⃣ Load Bronze Data
df_country = spark.read.option("header", True).csv(bronze_country_path)
df_city = spark.read.option("header", True).csv(bronze_city_path)
df_weather = spark.read.option("header", True).csv(bronze_weather_path)

# 5️⃣ Clean and Transform
df_country_clean = df_country.na.fill({
        "name": "UNKNOWN",
        "iso2": "XX",
        "capital": "UNKNOWN",
        "continent": "UNKNOWN"
    }) \
    .withColumn("name", initcap(trim(col("name")))) \
    .withColumn("iso2", upper(trim(col("iso2")))) \
    .withColumn("capital", initcap(trim(col("capital")))) \
    .withColumn("continent", upper(trim(col("continent")))) \
    .dropDuplicates()

df_city_clean = df_city.na.fill({
        "city_name": "UNKNOWN",
        "country_iso2": "XX",
        "iata_code": "XXX",
        "timezone": "UTC"
    }) \
    .withColumn("city_name", initcap(trim(col("city_name")))) \
    .withColumn("country_iso2", upper(trim(col("country_iso2")))) \
    .withColumn("iata_code", upper(trim(col("iata_code")))) \
    .dropDuplicates()

df_weather_clean = df_weather.na.fill({
        "city": "UNKNOWN",
        "country": "UNKNOWN",
        "region": "UNKNOWN",
        "weather_desc": "UNKNOWN"
    }) \
    .withColumn("city", initcap(trim(col("city")))) \
    .withColumn("country", upper(trim(col("country")))) \
    .withColumn("region", initcap(trim(col("region")))) \
    .withColumn("weather_desc", upper(trim(col("weather_desc")))) \
    .dropDuplicates()

# 6️⃣ Ensure Namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_layer")

# 7️⃣ Country MERGE using DataFrame join logic
try:
    df_existing_country = spark.read.format("iceberg").load("nessie.silver_layer.countries_data")
    df_merged_country = df_existing_country.alias("target").join(
        df_country_clean.alias("source"), "id", "outer"
    ).select(
        coalesce(col("source.id"), col("target.id")).alias("id"),
        coalesce(col("source.name"), col("target.name")).alias("name"),
        coalesce(col("source.iso2"), col("target.iso2")).alias("iso2"),
        coalesce(col("source.capital"), col("target.capital")).alias("capital"),
        coalesce(col("source.continent"), col("target.continent")).alias("continent"),
        coalesce(col("source.population"), col("target.population")).alias("population"),
        coalesce(col("source.created_at"), col("target.created_at")).alias("created_at"),
        coalesce(col("source.updated_at"), col("target.updated_at")).alias("updated_at")
    )
    df_merged_country.writeTo("nessie.silver_layer.countries_data").overwritePartitions()
except:
    df_country_clean.writeTo("nessie.silver_layer.countries_data").createOrReplace()

# 8️⃣ City MERGE using DataFrame join logic
try:
    df_existing_city = spark.read.format("iceberg").load("nessie.silver_layer.cities_data")
    df_merged_city = df_existing_city.alias("target").join(
        df_city_clean.alias("source"), "id", "outer"
    ).select(
        coalesce(col("source.id"), col("target.id")).alias("id"),
        coalesce(col("source.iata_code"), col("target.iata_code")).alias("iata_code"),
        coalesce(col("source.city_name"), col("target.city_name")).alias("city_name"),
        coalesce(col("source.country_iso2"), col("target.country_iso2")).alias("country_iso2"),
        coalesce(col("source.latitude"), col("target.latitude")).alias("latitude"),
        coalesce(col("source.longitude"), col("target.longitude")).alias("longitude"),
        coalesce(col("source.timezone"), col("target.timezone")).alias("timezone"),
        coalesce(col("source.created_at"), col("target.created_at")).alias("created_at"),
        coalesce(col("source.updated_at"), col("target.updated_at")).alias("updated_at")
    )
    df_merged_city.writeTo("nessie.silver_layer.cities_data").overwritePartitions()
except:
    df_city_clean.writeTo("nessie.silver_layer.cities_data").createOrReplace()

# 9️⃣ Weather Data - Append Only (Daily)
try:
    df_weather_clean.writeTo("nessie.silver_layer.weather_data").append()
except:
    df_weather_clean.writeTo("nessie.silver_layer.weather_data").createOrReplace()

# 🔟 Optional: Verify
print("✅ Countries Table Preview:")
spark.read.table("nessie.silver_layer.countries_data").show(5)

print("✅ Cities Table Preview:")
spark.read.table("nessie.silver_layer.cities_data").show(5)

print("✅ Weather Table Preview:")
spark.read.table("nessie.silver_layer.weather_data").show(5)

spark.stop()
