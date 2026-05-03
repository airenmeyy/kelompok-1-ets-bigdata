import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- 1. KONFIGURASI ELITE SPARK SESSION (Delta Lake + RocksDB) ---
# Ini adalah setup standar industri untuk menangani data besar secara aman (ACID)
# dan efisien dalam pengelolaan memori.
spark = SparkSession.builder \
    .appName("GempaRadar-Ultimate-Engine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .getOrCreate()

# Mengurangi noise log
spark.sparkContext.setLogLevel("ERROR")

# --- 2. SCHEMA ENFORCEMENT ---
# Menjamin integritas data agar pipeline tidak crash jika format sumber berubah
api_schema = StructType([
    StructField("id", StringType()),
    StructField("event_time", StringType()),
    StructField("magnitude", DoubleType()),
    StructField("depth_km", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("place", StringType()),
    StructField("tsunami", IntegerType())
])

# --- 3. GEOSPATIAL INTELLIGENCE (Haversine Formula) ---
# Menghitung jarak pusat gempa ke Jakarta secara real-time
JKT_LAT, JKT_LON = -6.1754, 106.8272

def calculate_dist():
    # Menggunakan fungsi matematis SQL bawaan Spark agar eksekusi sangat cepat (vektorisasi)
    return (2 * 6371 * asin(sqrt(
        pow(sin(radians(lit(JKT_LAT) - col("latitude")) / 2), 2) +
        cos(radians(lit(JKT_LAT))) * cos(radians(col("latitude"))) *
        pow(sin(radians(lit(JKT_LON) - col("longitude")) / 2), 2)
    )))

# --- 4. INGESTION LAYER (KAFKA STREAMING) ---
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "gempa-api") \
    .option("startingOffsets", "earliest") \
    .load()

# --- 5. ADVANCED PROCESSING & HEURISTIC ML SCORING ---
df_processed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), api_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .withWatermark("event_time", "10 minutes")

# Fitur-fitur World Class:
df_ultimate = df_processed \
    .withColumn("dist_to_jakarta_km", calculate_dist()) \
    .withColumn("tsunami_danger_score", 
        # Heuristic Scoring: Semakin besar Magnitudo dan semakin dangkal (Depth kecil), skor semakin tinggi
        (col("magnitude") * 1.5) + (100 / (col("depth_km") + 1))
    ) \
    .withColumn("status_siaga", 
        when(col("tsunami_danger_score") > 12, "🔴 AWAS (BAHAYA TINGGI)")
        .when(col("tsunami_danger_score") > 9, "🟡 SIAGA (MENENGAH)")
        .otherwise("🟢 WASPADA (RENDAH)")
    )

# --- 6. WRITING TO DELTA LAKE (GOLD STANDARD STORAGE) ---
# Delta Lake memungkinkan ACID transactions dan Time Travel di HDFS
query_delta = df_ultimate.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://hadoop-namenode:8020/checkpoints/gempa_ultimate_v1") \
    .start("hdfs://hadoop-namenode:8020/data/gempa/delta/quakes")

# --- 7. REAL-TIME MONITORING CONSOLE ---
query_console = df_ultimate.select(
    date_format("event_time", "HH:mm:ss").alias("Waktu"),
    "magnitude", 
    round("depth_km", 1).alias("depth"),
    "status_siaga",
    round("dist_to_jakarta_km", 0).alias("jarak_jkt_km"),
    "place"
).writeStream \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("append") \
    .start()

print("🚀 Ultimate Spark Engine is Running... Monitoring Real-Time Events...")
spark.streams.awaitAnyTermination()
