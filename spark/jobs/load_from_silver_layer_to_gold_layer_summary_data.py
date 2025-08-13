from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ================= Spark Setup =================
spark = SparkSession.builder \
    .appName("Gold Layer Transformation") \
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

# ================= Read Silver Layer Tables =================
flight_df = spark.table("nessie.silver_layer.flight_data")
airline_df = spark.table("nessie.silver_layer.airline_data")
airport_df = spark.table("nessie.silver_layer.airport_data")
countries_df = spark.table("nessie.silver_layer.countries_data")
cities_df = spark.table("nessie.silver_layer.cities_data")
#weather_df = spark.table("nessie.silver_layer.weather_data")

# ================= Delete Existing Gold Tables =================
gold_tables = [
    "nessie.gold_layer.flight_performance_summary_v1",
    "nessie.gold_layer.airport_capacity_analysis_v1",
    "nessie.gold_layer.city_weather_analysis_v1"
]

for table in gold_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"Dropped existing table: {table}")
    except Exception as e:
        print(f"Error dropping table {table}: {str(e)}")

# ================== 1. Flight Performance Summary ==================
flight_perf_df = flight_df.join(
    airline_df, 
    flight_df.airline_iata == airline_df.iata, 
    "left"
) \
# .join(  # Weather join removed
#     weather_df, 
#     flight_df.departure_airport_iata == weather_df.city, 
#     "left"
# ) \
.select(
    flight_df.flight_id,
    flight_df.flight_number,
    flight_df.airline_iata,
    airline_df.name.alias("airline_name"),
    flight_df.aircraft_type,
    flight_df.departure_airport_iata,
    flight_df.arrival_airport_iata,
    flight_df.distance_km,
    flight_df.status.alias("flight_status"),
    # weather_df.weather_desc.alias("weather_condition"),
    # weather_df.temperature_c,
    # weather_df.wind_speed_kmh
)

flight_perf_df.writeTo("nessie.gold_layer.flight_performance_summary_v1").createOrReplace()
print("Flight Performance Summary created in Iceberg")

# ================== 2. Airport Capacity Analysis ==================
airport_analysis_df = airport_df.join(
    countries_df, 
    airport_df.country_iso2 == countries_df.iso2, 
    "left"
).select(
    airport_df.airport_id,
    airport_df.airport_name,
    airport_df.country_iso2,
    countries_df.name.alias("country_name"),
    countries_df.continent,
    countries_df.population
)

airport_analysis_df.writeTo("nessie.gold_layer.airport_capacity_analysis_v1").createOrReplace()
print("Airport Capacity Analysis created in Iceberg")

# ================== 3. City Weather Analysis ==================
# city_weather_df = cities_df.join(
#     weather_df, 
#     cities_df.iata_code == weather_df.city, 
#     "left"
# ).join(
#     countries_df, 
#     cities_df.country_iso2 == countries_df.iso2, 
#     "left"
# ).select(
#     cities_df.city_name,
#     cities_df.country_iso2,
#     countries_df.name.alias("country_name"),
#     cities_df.latitude,
#     cities_df.longitude,
#     weather_df.temperature_c,
#     weather_df.humidity,
#     weather_df.wind_speed_kmh,
#     weather_df.weather_desc
# )

# city_weather_df.writeTo("nessie.gold_layer.city_weather_analysis_v1").createOrReplace()
# print("City Weather Analysis created in Iceberg")

# ================== Pipeline Complete ==================
print("Gold Layer Pipeline Complete - All tables recreated in Iceberg")
spark.stop()
