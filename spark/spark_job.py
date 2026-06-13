from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, when, lit, last
from pyspark.sql.types import *
from datetime import datetime

# ─────────────────────────────────────────
# INISIALISASI SPARK SESSION
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("AIRA-AQI-Processing") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 50)
print("AIRA Spark Job Started")
print("=" * 50)

HDFS = "hdfs://namenode:9000"

# ─────────────────────────────────────────
# 1. BACA SEMUA DATA DARI HDFS
# ─────────────────────────────────────────
print("\n[1] Reading data from HDFS...")

df_aqi = spark.read.json(f"{HDFS}/aira/raw/aqi/*/*/*/*/*.jsonl")
print(f"    AQI records     : {df_aqi.count()}")

# Baca traffic kalau ada
try:
    df_traffic = spark.read.json(f"{HDFS}/aira/raw/traffic/*/*/*/*/*.jsonl")
    print(f"    Traffic records : {df_traffic.count()}")
    has_traffic = True
except:
    has_traffic = False
    print("    Traffic records : 0 (skipped)")

# Baca UV kalau ada
try:
    df_uv = spark.read.json(f"{HDFS}/aira/raw/uv/*/*/*/*/*.jsonl")
    print(f"    UV records      : {df_uv.count()}")
    has_uv = True
except:
    has_uv = False
    print("    UV records      : 0 (skipped)")

# Baca weather kalau ada
try:
    df_weather = spark.read.json(f"{HDFS}/aira/raw/weather/*/*/*/*/*.jsonl")
    print(f"    Weather records : {df_weather.count()}")
    has_weather = True
except:
    has_weather = False
    print("    Weather records : 0 (skipped)")

# ─────────────────────────────────────────
# 2. TRANSFORMASI AQI
# ─────────────────────────────────────────
print("\n[2] Transforming AQI data...")

df_aqi_clean = df_aqi \
    .withColumn("aqi",       col("aqi").cast(IntegerType())) \
    .withColumn("pm25",      col("pm25").cast(FloatType())) \
    .withColumn("temp",      col("temp").cast(FloatType())) \
    .withColumn("humidity",  col("humidity").cast(FloatType())) \
    .withColumn("wind",      col("wind").cast(FloatType())) \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .dropna(subset=["aqi", "station", "timestamp"]) \
    .withColumn("aqi_category",
        when(col("aqi") <= 50,  lit("Good"))
        .when(col("aqi") <= 100, lit("Moderate"))
        .when(col("aqi") <= 150, lit("Unhealthy for Sensitive Groups"))
        .when(col("aqi") <= 200, lit("Unhealthy"))
        .when(col("aqi") <= 300, lit("Very Unhealthy"))
        .otherwise(lit("Hazardous"))
    )

# ─────────────────────────────────────────
# 3. AGREGASI AQI PER STASIUN
# ─────────────────────────────────────────
print("\n[3] Aggregating AQI by station...")

df_station = df_aqi_clean.groupBy("station").agg(
    avg("aqi").alias("avg_aqi"),
    max("aqi").alias("max_aqi"),
    min("aqi").alias("min_aqi"),
    avg("pm25").alias("avg_pm25"),
    avg("temp").alias("avg_temp"),
    avg("humidity").alias("avg_humidity"),
    count("*").alias("total_records")
).orderBy("avg_aqi", ascending=False)

print("    AQI Summary:")
df_station.show()

# ─────────────────────────────────────────
# 4. AGREGASI TRAFFIC
# ─────────────────────────────────────────
if has_traffic:
    print("\n[4] Aggregating traffic data...")
    df_traffic_clean = df_traffic \
        .withColumn("congestion_pct",   col("congestion_pct").cast(FloatType())) \
        .withColumn("current_speed",    col("current_speed").cast(FloatType())) \
        .withColumn("free_flow_speed",  col("free_flow_speed").cast(FloatType())) \
        .dropna(subset=["point"])

    df_traffic_summary = df_traffic_clean.groupBy("point").agg(
        avg("congestion_pct").alias("avg_congestion"),
        max("congestion_pct").alias("max_congestion"),
        avg("current_speed").alias("avg_speed"),
        avg("free_flow_speed").alias("free_flow_speed"),
        count("*").alias("total_records")
    ).withColumn("traffic_status",
        when(col("avg_congestion") >= 60, lit("Heavy"))
        .when(col("avg_congestion") >= 30, lit("Moderate"))
        .otherwise(lit("Light"))
    ).orderBy("avg_congestion", ascending=False)

    print("    Traffic Summary:")
    df_traffic_summary.show()

# ─────────────────────────────────────────
# 5. AGREGASI UV
# ─────────────────────────────────────────
if has_uv:
    print("\n[5] Aggregating UV data...")
    df_uv_clean = df_uv \
        .withColumn("uv",     col("uv").cast(FloatType())) \
        .withColumn("uv_max", col("uv_max").cast(FloatType())) \
        .withColumn("ozone",  col("ozone").cast(FloatType()))

    df_uv_summary = df_uv_clean.agg(
        avg("uv").alias("avg_uv"),
        max("uv_max").alias("max_uv"),
        avg("ozone").alias("avg_ozone"),
        count("*").alias("total_records")
    )

    print("    UV Summary:")
    df_uv_summary.show()

# ─────────────────────────────────────────
# 5b. AGREGASI WEATHER
# ─────────────────────────────────────────
if has_weather:
    print("\n[5b] Aggregating weather data...")
    df_weather_clean = df_weather \
        .withColumn("temp",       col("temp").cast(FloatType())) \
        .withColumn("feels_like", col("feels_like").cast(FloatType())) \
        .withColumn("humidity",   col("humidity").cast(FloatType())) \
        .withColumn("wind_speed", col("wind_speed").cast(FloatType())) \
        .withColumn("clouds",     col("clouds").cast(FloatType())) \
        .withColumn("rain_1h",    col("rain_1h").cast(FloatType())) \
        .dropna(subset=["temp"])

    df_weather_summary = df_weather_clean.groupBy("station").agg(
        avg("temp").alias("avg_temp"),
        avg("feels_like").alias("avg_feels_like"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("clouds").alias("avg_clouds"),
        avg("rain_1h").alias("avg_rain"),
        last("weather_desc").alias("weather_desc"),
        count("*").alias("total_records")
    )

    print("    Weather Summary:")
    df_weather_summary.show()


# ─────────────────────────────────────────
# 6. PREDIKSI AQI (dengan faktor traffic)
# ─────────────────────────────────────────
print("\n[6] Generating predictions...")

df_prediction = df_aqi_clean.groupBy("station").agg(
    avg("aqi").alias("current_avg_aqi"),
    avg("humidity").alias("avg_humidity"),
    avg("wind").alias("avg_wind"),
    last("lat").alias("lat"),
    last("lon").alias("lon"),
)

# Faktor traffic — rata-rata kemacetan semua koridor
if has_traffic:
    avg_congestion = df_traffic_clean.agg(
        avg("congestion_pct")
    ).collect()[0][0] or 0
else:
    avg_congestion = 0

congestion_factor = lit(float(avg_congestion))

df_prediction = df_prediction.withColumn("predicted_aqi_next_hour",
    when((col("avg_humidity") > 70) & (col("avg_wind") < 2),
         ((col("current_avg_aqi") * 1.1) + (congestion_factor * 0.1)).cast(IntegerType()))
    .when(col("avg_wind") > 5,
         (col("current_avg_aqi") * 0.9).cast(IntegerType()))
    .otherwise(
         (col("current_avg_aqi") + (congestion_factor * 0.05)).cast(IntegerType()))
).withColumn("prediction_category",
    when(col("predicted_aqi_next_hour") <= 50,  lit("Good"))
    .when(col("predicted_aqi_next_hour") <= 100, lit("Moderate"))
    .when(col("predicted_aqi_next_hour") <= 150, lit("Unhealthy for Sensitive Groups"))
    .when(col("predicted_aqi_next_hour") <= 200, lit("Unhealthy"))
    .otherwise(lit("Very Unhealthy"))
)

print("    Predictions:")
df_prediction.show()

# ─────────────────────────────────────────
# 6b. TIME SERIES — AQI per jam
# ─────────────────────────────────────────
print("\n[6b] Building time series...")

from pyspark.sql.functions import date_format, hour, to_timestamp

df_timeseries = df_aqi_clean \
    .withColumn("hour_bucket", date_format(col("timestamp"), "yyyy-MM-dd HH:00")) \
    .groupBy("hour_bucket") \
    .agg(
        avg("aqi").alias("avg_aqi"),
        max("aqi").alias("max_aqi"),
        min("aqi").alias("min_aqi"),
        avg("pm25").alias("avg_pm25"),
        count("*").alias("total_records")
    ) \
    .orderBy("hour_bucket")

print("    Time Series:")
df_timeseries.show(10)

# ─────────────────────────────────────────
# 7. SIMPAN SEMUA HASIL KE HDFS
# ─────────────────────────────────────────
print("\n[7] Saving results to HDFS...")

output = f"{HDFS}/aira/processed"

df_station.write.mode("overwrite").json(f"{output}/station_summary")
print(f"    ✓ Station summary saved")

df_prediction.write.mode("overwrite").json(f"{output}/predictions")
print(f"    ✓ Predictions saved")

if has_traffic:
    df_traffic_summary.write.mode("overwrite").json(f"{output}/traffic_summary")
    print(f"    ✓ Traffic summary saved")

if has_uv:
    df_uv_summary.write.mode("overwrite").json(f"{output}/uv_summary")
    print(f"    ✓ UV summary saved")

if has_weather:
    df_weather_summary.write.mode("overwrite").json(f"{output}/weather_summary")
    print(f"    ✓ Weather summary saved")

df_timeseries.write.mode("overwrite").json(f"{output}/timeseries")
print(f"    ✓ Time series saved")

print("\n" + "=" * 50)
print("AIRA Spark Job Completed!")
print("=" * 50)

spark.stop()
