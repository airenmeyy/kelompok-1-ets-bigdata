import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# --- 1. KONFIGURASI ELITE SPARK SESSION (Delta Lake + RocksDB) ---
# Setup standar industri untuk menangani data besar secara aman (ACID) dan efisien.
spark = SparkSession.builder \
    .appName("GempaRadar-Super-Sempurna") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- 2. BATCH ANALYSIS: MEMBACA DARI HDFS (Sesuai Rubrik Dimensi 3) ---
# Rubrik: "Spark membaca dari HDFS (bukan file lokal)"
print("\n" + "="*60)
print("🧠 BPBD ANALYTICS REPORT: BATCH PROCESSING FROM HDFS")
print("="*60)

try:
    hdfs_path = "hdfs://hadoop-namenode:8020/data/gempa/api/*.json"
    df_hdfs = spark.read.json(hdfs_path)
    
    if df_hdfs.count() > 0:
        # A. Distribusi Magnitudo (Analisis Wajib 1)
        print("\n[1] DISTRIBUSI MAGNITUDO (Analisis Keamanan)")
        df_mag = df_hdfs.withColumn("kategori_mag", 
            when(col("magnitude") < 3, "Mikro (<3)")
            .when((col("magnitude") >= 3) & (col("magnitude") < 4), "Minor (3-4)")
            .when((col("magnitude") >= 4) & (col("magnitude") < 5), "Sedang (4-5)")
            .otherwise("Kuat (>5)")
        ).groupBy("kategori_mag").count().orderBy("count", ascending=False)
        df_mag.show()

        # B. Wilayah Paling Aktif (Analisis Wajib 2 - Top 10)
        print("\n[2] TOP 10 WILAYAH PALING AKTIF SEISMIK")
        df_active = df_hdfs.withColumn("wilayah", 
            regexp_replace(col("place"), ".* of ", "") # Membersihkan string lokasi
        ).groupBy("wilayah").count().orderBy("count", ascending=False).limit(10)
        df_active.show()

        # C. Distribusi Kedalaman (Analisis Wajib 3)
        print("\n[3] ANALISIS KEDALAMAN GEMPA (Dangkal vs Dalam)")
        depth_stats = df_hdfs.select(
            avg("depth_km").alias("rata_rata_depth"),
            count(when(col("depth_km") < 70, 1)).alias("dangkal_count"),
            count(when(col("depth_km") >= 70, 1)).alias("dalam_count")
        )
        depth_stats.show()

        # --- BONUS +5: SPARK MLLIB (LINEAR REGRESSION) ---
        print("\n[BONUS] SPARK MLLIB: TREN MAGNITUDO (Linear Regression)")
        df_ml = df_hdfs.select("time_epoch", "magnitude").dropna()
        if df_ml.count() > 1:
            assembler = VectorAssembler(inputCols=["time_epoch"], outputCol="features")
            ml_data = assembler.transform(df_ml)
            lr = LinearRegression(featuresCol="features", labelCol="magnitude")
            lr_model = lr.fit(ml_data)
            print(f"> Prediksi Tren: Magnitudo cenderung {'naik' if lr_model.coefficients[0] > 0 else 'turun'} seiring waktu.")

        # --- EXPORT HASIL KE JSON (Sesuai Rubrik) ---
        results = {
            "total_gempa": df_hdfs.count(),
            "rata_rata_kedalaman": depth_stats.collect()[0]["rata_rata_depth"],
            "wilayah_teraktif": df_active.first()["wilayah"] if df_active.count() > 0 else "N/A"
        }
        # Simpan ringkasan ke HDFS
        summary_df = spark.createDataFrame([results])
        summary_df.write.mode("overwrite").json("hdfs://hadoop-namenode:8020/data/gempa/hasil/spark_results.json")
        print("\n✅ Hasil analisis Batch disimpan ke: /data/gempa/hasil/spark_results.json")
    else:
        print("⚠️ Data di HDFS belum tersedia untuk Batch Analysis.")
except Exception as e:
    print(f"⚠️ Gagal melakukan Batch Analysis (HDFS mungkin kosong): {e}")

print("\n" + "="*60)
print("🚀 SWITCHING TO REAL-TIME STREAMING ENGINE (KAFKA)")
print("="*60)

# --- 3. REAL-TIME STREAMING LOGIC (Tetap dipertahankan untuk Dashboard) ---
api_schema = StructType([
    StructField("id", StringType()),
    StructField("event_time", StringType()),
    StructField("magnitude", DoubleType()),
    StructField("depth_km", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("place", StringType())
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "gempa-api") \
    .option("startingOffsets", "earliest") \
    .load()

df_processed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), api_schema).alias("data")) \
    .select("data.*")

df_ultimate = df_processed \
    .withColumn("tsunami_score", (col("magnitude") * 1.5) + (100 / (col("depth_km") + 1))) \
    .withColumn("status_siaga", 
        when(col("tsunami_score") > 12, "🔴 AWAS")
        .when(col("tsunami_score") > 9, "🟡 SIAGA")
        .otherwise("🟢 WASPADA")
    )

# Simpan ke Delta Lake (HDFS)
query_delta = df_ultimate.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://hadoop-namenode:8020/checkpoints/gempa_v2") \
    .start("hdfs://hadoop-namenode:8020/data/gempa/delta/quakes")

# Console Output
query_console = df_ultimate.select("magnitude", "status_siaga", "place") \
    .writeStream \
    .format("console") \
    .start()

spark.streams.awaitAnyTermination()
