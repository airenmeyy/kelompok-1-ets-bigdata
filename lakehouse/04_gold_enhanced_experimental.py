"""
EXPERIMENTAL
Script: 04_gold_enhanced.py
Deskripsi: Gold Layer Enhancement — Mengimplementasikan metrik bisnis baru
           (Risk Score wilayah & Significant Alerts) dengan menggabungkan
           data spasial dan data temporal dari Silver API & Silver RSS.
Tugas: Anggota 4 / Kelanjutan Tahap Enhancement
Project: GempaRadar Data Lakehouse | Kelompok 1 ETS Big Data
"""

import os
import sys
import shutil

# Pindahkan CWD ke folder lakehouse/ agar path relatif bersifat portabel
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# ====================================================================
# AUTO-REDIRECT KE DOCKER CONTAINER (Konsisten dengan standar kelompok)
# ====================================================================
if os.name == 'nt' and not os.environ.get('HADOOP_HOME'):
    import subprocess
    print("\n" + "="*60)
    print("  GOLD LAYER ENHANCEMENT — GEMPARADAR DATA LAKEHOUSE")
    print("="*60)
    print("[*] Mendeteksi Windows Host tanpa HADOOP_HOME.")
    print("[*] Mengalihkan eksekusi ke dalam container Docker 'spark-master'...")

    container_script = "/app/lakehouse/04_gold_enhanced.py"
    cmd = [
        "docker", "exec", "-u", "root", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--packages", "io.delta:delta-spark_2.12:3.1.0",
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        container_script
    ]
    try:
        result = subprocess.run(cmd)
        sys.exit(result.returncode)
    except Exception as e:
        print(f"[-] Gagal mengalihkan ke container Docker: {e}")
        sys.exit(1)

print("\n" + "="*60)
print("  GOLD LAYER ENHANCEMENT — GEMPARADAR DATA LAKEHOUSE")
print("  Mengimplementasikan Analisis Tingkat Lanjut & Korelasi Berita")
print("="*60)

# ====================================================================
# IMPORT LIBRARY
# ====================================================================
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, when, count, avg, regexp_replace, expr,
        round as spark_round, lit, current_timestamp
    )
    from delta import configure_spark_with_delta_pip
    HAS_DELTA_PACKAGE = True
except Exception:
    HAS_DELTA_PACKAGE = False

# ====================================================================
# INISIALISASI SPARKSESSION
# ====================================================================
print("[*] Menginisialisasi SparkSession dengan ekstensi Delta Lake...")
os.environ["HADOOP_USER_NAME"] = "hadoop"

try:
    builder = SparkSession.builder \
        .appName("Gold-Enhanced-GempaRadar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020")

    if HAS_DELTA_PACKAGE:
        spark = configure_spark_with_delta_pip(builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]).getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("[+] SparkSession berhasil dibuat!")
except Exception as e:
    print(f"[-] Gagal menginisialisasi SparkSession: {e}")
    sys.exit(1)

# ====================================================================
# DEFINISI PATH INPUT & OUTPUT
# ====================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAKEHOUSE_DATA_DIR = os.path.join(BASE_DIR, "lakehouse_data")

def local_path(relative):
    abs_path = os.path.join(LAKEHOUSE_DATA_DIR, relative)
    return f"file://{abs_path.replace(os.sep, '/')}"

# INPUT: Dari Silver Layer
SILVER_API_PATH = local_path("silver/gempa_api")
SILVER_RSS_PATH = local_path("silver/gempa_rss")

# OUTPUT: Sesuai Standar Baru Gold Layer
GOLD_MAG_DIST_PATH        = local_path("gold/gempa_mag_dist")
GOLD_REGION_RANK_PATH     = local_path("gold/gempa_region_rank")
GOLD_RISK_SCORE_PATH      = local_path("gold/gempa_risk_score")
GOLD_SIGNIFICANT_ALERTS_PATH = local_path("gold/gempa_significant_alerts")

def clean_gold_path(delta_uri):
    clean = delta_uri.replace("file://", "")
    if os.path.exists(clean):
        try: shutil.rmtree(clean)
        except Exception: pass

# ====================================================================
# BACA DATA SILVER
# ====================================================================
print("\n[*] Membaca data dari Silver Layer...")
try:
    df_silver_api = spark.read.format("delta").load(SILVER_API_PATH)
    df_silver_rss = spark.read.format("delta").load(SILVER_RSS_PATH)
    print(f"[+] Load Sukses! Silver API ({df_silver_api.count()} baris), Silver RSS ({df_silver_rss.count()} baris)")
except Exception as e:
    print(f"[-] Gagal membaca data Silver: {e}. Pastikan script 02_silver.py sudah sukses dijalankan.")
    spark.stop()
    sys.exit(1)

# Pra-proses internal: Ekstrak kolom 'wilayah' secara seragam dari kolom 'place'
df_api_enhanced = df_silver_api.withColumn("wilayah", regexp_replace(col("place"), '.* of ', ''))


# ====================================================================
# 1. TABEL: gold/gempa_mag_dist (Reproduksi Distribusi Magnitudo)
# ====================================================================
print("\n" + "-"*50 + "\n[*] Memproses: gold/gempa_mag_dist\n" + "-"*50)
df_mag_dist = df_api_enhanced.groupBy("mag_category") \
    .count().orderBy("count", ascending=False)

clean_gold_path(GOLD_MAG_DIST_PATH)
df_mag_dist.withColumn("_gold_processed_at", current_timestamp()) \
    .withColumn("_layer", lit("gold")) \
    .write.format("delta").mode("overwrite").save(GOLD_MAG_DIST_PATH)
df_mag_dist.show()


# ====================================================================
# 2. TABEL: gold/gempa_region_rank (Reproduksi Wilayah Paling Aktif)
# ====================================================================
print("\n" + "-"*50 + "\n[*] Memproses: gold/gempa_region_rank\n" + "-"*50)
df_region_rank = df_api_enhanced.groupBy("wilayah").agg(
    count("*").alias("jumlah_gempa"),
    spark_round(avg("magnitude"), 2).alias("rata_mag"),
    spark_round(avg("depth_km"), 1).alias("rata_depth_km")
).orderBy("jumlah_gempa", ascending=False)

clean_gold_path(GOLD_REGION_RANK_PATH)
df_region_rank.withColumn("_gold_processed_at", current_timestamp()) \
    .withColumn("_layer", lit("gold")) \
    .write.format("delta").mode("overwrite").save(GOLD_REGION_RANK_PATH)
df_region_rank.show(5)


# ====================================================================
# 3. TABEL: gold/gempa_risk_score (🆕 ENHANCED: Analisis Risiko Wilayah)
# ====================================================================
print("\n" + "-"*50 + "\n[*] Memproses: gold/gempa_risk_score\n" + "-"*50)

# Formula: Frekuensi * Rata Magnitude * (1 + % Gempa Dangkal)
# Kolom depth disesuaikan dengan skema real Anda: depth_km
df_risk_score = df_api_enhanced.groupBy("wilayah").agg(
    count("*").alias("frekuensi"),
    spark_round(avg("magnitude"), 2).alias("avg_mag"),
    spark_round((count(when(col("depth_km") < 70, True)) / count("*")), 4).alias("pct_dangkal")
).withColumn(
    "risk_score",
    spark_round(col("frekuensi") * col("avg_mag") * (1 + col("pct_dangkal")), 2)
).orderBy("risk_score", ascending=False)

clean_gold_path(GOLD_RISK_SCORE_PATH)
df_risk_score.withColumn("_gold_processed_at", current_timestamp()) \
    .withColumn("_layer", lit("gold")) \
    .write.format("delta").mode("overwrite").save(GOLD_RISK_SCORE_PATH)
df_risk_score.show(10)


# ====================================================================
# 4. TABEL: gold/gempa_significant_alerts (🆕 ENHANCED: Korelasi Berita)
# ====================================================================
print("\n" + "-"*50 + "\n[*] Memproses: gold/gempa_significant_alerts\n" + "-"*50)

# Filter Gempa Signifikan (M > 4.5)
df_api_sig = df_api_enhanced.filter(col("magnitude") > 4.5)

# Kondisi Join Temporal: Berita (RSS) terbit di antara rentang waktu kejadian gempa hingga 2 jam setelahnya
join_condition = [
    df_silver_rss["published_time"] >= df_api_sig["event_time"],
    df_silver_rss["published_time"] <= (df_api_sig["event_time"] + expr("INTERVAL 2 HOURS"))
]

df_alerts = df_api_sig.join(df_silver_rss, join_condition, "inner") \
    .select(
        df_api_sig["id"].alias("gempa_id"),
        col("wilayah"),
        col("magnitude"),
        col("depth_km"),
        col("event_time").alias("waktu_gempa"),
        df_silver_rss["id"].alias("berita_id"),
        col("title").alias("judul_berita"),
        col("published_time").alias("waktu_berita"),
        col("summary_clean")
    ).orderBy("waktu_gempa", ascending=False)

clean_gold_path(GOLD_SIGNIFICANT_ALERTS_PATH)
df_alerts.withColumn("_gold_processed_at", current_timestamp()) \
    .withColumn("_layer", lit("gold")) \
    .write.format("delta").mode("overwrite").save(GOLD_SIGNIFICANT_ALERTS_PATH)

if df_alerts.count() > 0:
    df_alerts.select("wilayah", "magnitude", "waktu_gempa", "judul_berita").show(5, truncate=False)
else:
    print("[!] Tidak ada berita RSS yang cocok dalam jendela 2 jam setelah gempa signifikan saat ini.")


# ====================================================================
# VERIFIKASI AKHIR
# ====================================================================
print("\n" + "="*50)
print("  VERIFIKASI PIPELINE ENHANCED GOLD LAYER")
print("="*50)
enhanced_tables = {
    "Kategori Magnitudo": GOLD_MAG_DIST_PATH,
    "Ranking Aktivitas Wilayah": GOLD_REGION_RANK_PATH,
    "Skor Risiko Wilayah": GOLD_RISK_SCORE_PATH,
    "Korelasi Berita Gempa": GOLD_SIGNIFICANT_ALERTS_PATH
}

for name, path in enhanced_tables.items():
    try:
        df_v = spark.read.format("delta").load(path)
        print(f"[+] {name:30s} -> {df_v.count():<5} baris [TERFORMAT DELTA LAKE] ✅")
    except Exception as e:
        print(f"[-] {name:30s} -> Error membaca ({e}) ❌")

print("\n[*] Menutup SparkSession...")
spark.stop()
print("="*60)
print("  Tahap Gold Layer Enhancement Selesai! Dashboard Siap Dikoneksikan.")
print("="*60 + "\n")
