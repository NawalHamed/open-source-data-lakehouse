from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, col

# ================= Spark Setup =================
spark = SparkSession.builder \
    .appName("Airline JSON to Iceberg Silver") \
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

# ================= Read Silver Layer Tables =================
#flight_df = spark.read.format("iceberg").load("nessie.silver_layer.flight_data")
#airline_df = spark.read.format("iceberg").load("nessie.silver_layer.airline_data")
#airport_df = spark.read.format("iceberg").load("nessie.silver_layer.airport_data")
#countries_df = spark.read.format("iceberg").load("nessie.silver_layer.countries_data")
#cities_df = spark.read.format("iceberg").load("nessie.silver_layer.cities_data")
#weather_df = spark.read.format("iceberg").load("nessie.silver_layer.weather_data")

flight_df = spark.table("nessie.silver_layer.flight_data")
airline_df = spark.table("nessie.silver_layer.airline_data")
airport_df = spark.table("nessie.silver_layer.airport_data")
countries_df = spark.table("nessie.silver_layer.countries_data")
cities_df = spark.table("nessie.silver_layer.cities_data")
weather_df = spark.table("nessie.silver_layer.weather_data")

# ================== 1. Flight Performance Summary in Iceberg ==================
flight_perf_df = flight_df.join(airline_df, flight_df.airline_iata == airline_df.iata, "left") \
                          .join(weather_df, flight_df.departure_airport_iata == weather_df.city, "left") \
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
                              weather_df.weather_desc.alias("weather_condition"),
                              weather_df.temperature_c,
                              weather_df.wind_speed_kmh
                          )

flight_perf_df.writeTo("nessie.gold_layer.flight_performance_summary").overwritePartitions()
print("✅ Flight Performance Summary (Iceberg) written.")

# ================== 2. Airport Capacity & Continent Analysis in Iceberg ==================
airport_analysis_df = airport_df.join(countries_df, airport_df.country_iso2 == countries_df.iso2, "left") \
                                .select(
                                    airport_df.airport_id,
                                    airport_df.airport_name,
                                    airport_df.country_iso2,
                                    countries_df.name.alias("country_name"),
                                    countries_df.continent,
                                    countries_df.population.alias("country_population"),
                                    airport_df.latitude,
                                    airport_df.longitude
                                )

airport_analysis_df.writeTo("nessie.gold_layer.airport_capacity_analysis").overwritePartitions()
print("✅ Airport Capacity Analysis (Iceberg) written.")

# ================== 3. City Weather Enrichment in Iceberg ==================
city_weather_df = cities_df.join(weather_df, cities_df.iata_code == weather_df.city, "left") \
                           .join(countries_df, cities_df.country_iso2 == countries_df.iso2, "left") \
                           .select(
                               cities_df.city_name,
                               cities_df.country_iso2,
                               countries_df.name.alias("country_name"),
                               countries_df.continent,
                               cities_df.latitude,
                               cities_df.longitude,
                               weather_df.temperature_c,
                               weather_df.humidity,
                               weather_df.wind_speed_kmh,
                               weather_df.weather_desc
                           )

city_weather_df.writeTo("nessie.gold_layer.city_weather_analysis").overwritePartitions()
print("✅ City Weather Analysis (Iceberg) written.")

# ================== 4. Additional KPI: Flights & Distance per Airline ==================
kpi_df = flight_df.groupBy("airline_iata") \
                  .agg(
                      count("*").alias("total_flights"),
                      sum("distance_km").alias("total_distance_km"),
                      avg("distance_km").alias("avg_distance_km")
                  ) \
                  .join(airline_df, flight_df.airline_iata == airline_df.iata, "left") \
                  .select(
                      airline_df.iata.alias("airline_iata"),
                      airline_df.name.alias("airline_name"),
                      col("total_flights"),
                      col("total_distance_km"),
                      col("avg_distance_km")
                  )

kpi_df.writeTo("nessie.gold_layer.airline_kpi_summary").overwritePartitions()
print("✅ Airline KPI Summary (Iceberg) written.")

# ================== Pipeline Complete ==================
print("🎉 Complete Gold Layer Pipeline with KPIs written in Iceberg.")
