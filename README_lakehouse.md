# Arsitektur Lakehouse — GempaRadar

## Arsitektur Sebelum Lakehouse
Berikut gambaran arsitektur kami sebelumnya:

```mermaid
flowchart TD
    subgraph SRC["📡 Sumber Data"]
        direction LR
        USGS["🌍 USGS Earthquake API\n— polling tiap 60 detik —"]
        RSS["📰 Google News RSS\n— BMKG via Google News —\n— polling tiap 5 menit —"]
    end

    subgraph KAFKA["🔄 Ingestion Layer · Apache Kafka (KRaft Mode)"]
        direction LR
        P1["producer_api.py"]
        P2["producer_rss.py"]
        T1[["topic: gempa-api\n(1 partition)"]]
        T2[["topic: gempa-rss\n(1 partition)"]]
        P1 --> T1
        P2 --> T2
    end

    subgraph HDFS["Storage Layer · HDFS (Hadoop 3)"]
        direction TB
        CON["consumer_to_hdfs.py"]
        D1["📁 /data/gempa/api/\nraw JSON files"]
        D2["📁 /data/gempa/rss/\nraw JSON files"]
        D3["📁 /data/gempa/hasil/\n(manual output)"]
        CON --> D1
        CON --> D2
    end

    subgraph SERVE["📊 Serving Layer"]
        FLASK["Flask Dashboard\n(baca langsung dari HDFS)"]
    end

    USGS --> P1
    RSS  --> P2
    T1   --> CON
    T2   --> CON
    D1   --> FLASK
    D2   --> FLASK
    D3   --> FLASK

    classDef source fill:#1e3a5f,stroke:#4a9eff,color:#e8f4fd
    classDef kafka  fill:#1a3a2a,stroke:#4caf74,color:#d4edda
    classDef hdfs   fill:#3a2a1a,stroke:#ff9944,color:#fde8c8
    classDef serve  fill:#2a1a3a,stroke:#bb86fc,color:#ede0ff

    class USGS,RSS source
    class P1,P2,T1,T2 kafka
    class CON,D1,D2,D3 hdfs
    class FLASK serve
```

###  Kondisi Infra Saat Ini

- **Ingestion:** Apache Kafka 3.9 (KRaft Mode), 2 topik dengan masing-masing 1 partisi.
- **Producers:** `producer_api.py` (polling USGS tiap menit) & `producer_rss.py` (polling RSS berita, filter pakai hash agar tidak duplikat).
- **Storage:** HDFS cluster kecil-kecilan (NameNode + DataNode di Docker).
- **Compute:** Script consumer Python biasa yang kerjanya cuma nulis raw JSON ke folder HDFS.
- **Dashboard:** Flask App yang langsung memaksa nge-read dan nge-parse ribuan file JSON dari HDFS tiap kali ada request.

---

## ✅ Arsitektur Sesudah

### Konsep Utama: Medallion Architecture + Apache Hudi

Kita bakal pakai pola **Medallion Architecture**, yaitu teknik membagi penyimpanan menjadi 3 zona tingkat kematangan data:

- **Bronze (Raw Ingest):** Tempat penampungan data mentah persis kayak yang dikirim sama Kafka. Formatnya kita ganti ke Parquet biar hemat space, tapi datanya masih kotor.
- **Silver (Cleaned & Enriched):** Data dari Bronze dibersihkan, dideduplikasi, tipe datanya disesuaikan, dan skemanya dikunci (*schema enforced*). Di sini data udah siap pakai buat analisis.
- **Gold (Business-Ready):** Data agregat hasil kalkulasi berat (misal: rata-rata magnitudo per hari, hotspot wilayah rawan). Dashboard Flask kita tinggal baca dari layer ini—auto cepet!

Biar layer-layer di atas mendukung transaksi ACID dan bisa di-update/delete layaknya tabel SQL biasa, kita pasang **Apache Hudi** di atas HDFS kita.

```mermaid
flowchart TD
    subgraph SRC["📡 Sumber Data"]
        direction LR
        USGS["🌍 USGS Earthquake API\n— real-time stream —"]
        RSS["📰 Google News RSS\n— BMKG / media nasional —"]
    end

    subgraph KAFKA["🔄 Ingestion Layer · Apache Kafka"]
        direction LR
        P1["producer_api.py"]
        P2["producer_rss.py"]
        T1[["topic: gempa-api"]]
        T2[["topic: gempa-rss"]]
        P1 --> T1
        P2 --> T2
    end

    subgraph LAKE["🏛️ Lakehouse · HDFS + Apache Hudi"]
        direction TB

        subgraph BRONZE["🥉 Bronze Layer — Raw Ingest"]
            direction LR
            B1["📂 /lakehouse/bronze/gempa_api/\nformat: Parquet + Hudi CoW\nschema: as-is dari Kafka"]
            B2["📂 /lakehouse/bronze/gempa_rss/\nformat: Parquet + Hudi CoW\nschema: as-is dari Kafka"]
        end

        subgraph SILVER["🥈 Silver Layer — Cleaned & Enriched"]
            direction LR
            S1["📂 /lakehouse/silver/gempa_events/\n• dedup by event ID\n• null handling\n• tipe data enforced\n• koordinat valid"]
            S2["📂 /lakehouse/silver/berita_gempa/\n• dedup by hash_id\n• strip HTML summary\n• timestamp normalisasi"]
        end

        subgraph GOLD["🥇 Gold Layer — Analytics-Ready"]
            direction LR
            G1["📂 /lakehouse/gold/gempa_daily_stats/\n• count per hari\n• rata-rata magnitudo\n• sebaran kedalaman"]
            G2["📂 /lakehouse/gold/hotspot_wilayah/\n• grid heatmap Indonesia\n• top-N daerah rawan"]
            G3["📂 /lakehouse/gold/korelasi_berita/\n• event ↔ berita\n• sentiment sederhana"]
        end
    end

    subgraph PROC["⚡ Processing Layer · Apache Spark"]
        direction LR
        SS["Structured Streaming\n(Bronze ← Kafka)"]
        BATCH["Batch Jobs\n(Bronze → Silver → Gold)"]
        META["📋 Hive Metastore\n(Schema Registry)"]
        SS <--> META
        BATCH <--> META
    end

    subgraph SERVE["📊 Serving Layer"]
        direction LR
        SPARKSQL["🔍 Spark SQL / Trino\n(ad-hoc query)"]
        FLASK["🌐 Flask Dashboard\n(query via Spark SQL)"]
        BI["📈 Apache Superset\n(opsional)"]
    end

    USGS --> P1
    RSS  --> P2
    T1   --> SS
    T2   --> SS
    SS   --> B1
    SS   --> B2
    B1   --> BATCH
    B2   --> BATCH
    BATCH --> S1 & S2
    S1   --> G1 & G3
    S2   --> G2 & G3
    G1 & G2 & G3 --> SPARKSQL
    SPARKSQL --> FLASK
    SPARKSQL --> BI

    classDef source  fill:#1e3a5f,stroke:#4a9eff,color:#e8f4fd
    classDef kafka   fill:#1a3a2a,stroke:#4caf74,color:#d4edda
    classDef bronze  fill:#3d2b00,stroke:#cd7f32,color:#ffe8b0
    classDef silver  fill:#1e2a2a,stroke:#aacfcf,color:#d4eeee
    classDef gold    fill:#2a2200,stroke:#ffd700,color:#fffacc
    classDef proc    fill:#2a1f00,stroke:#ff9800,color:#fff3e0
    classDef serve   fill:#2a1a3a,stroke:#bb86fc,color:#ede0ff

    class USGS,RSS source
    class P1,P2,T1,T2 kafka
    class B1,B2 bronze
    class S1,S2 silver
    class G1,G2,G3 gold
    class SS,BATCH,META proc
    class SPARKSQL,FLASK,BI serve
```

### Komponen Tambahan

Untuk menyokong arsitektur  ini, ada beberapa tools baru yang kami instal:

- **Apache Hudi (Copy-on-Write / Merge-on-Read):** Sebagai format tabel utama. Bertanggung jawab membuat HDFS biasa jadi punya kekuatan layaknya database SQL (bisa insert, update, delete, dan time travel).
- **Apache Spark 3.x:** Buat nge-stream data dari Kafka ke Bronze secara real-time, dan running script ETL (Extract, Transform, Load) harian ke Silver dan Gold.
- **Hive Metastore:** Bertugas sebagai pustakawan skema tabel  *(Schema Registry)*. Jadi semua metadata lokasi file dan format kolom dicatat rapi di sini.
- **Trino (dulu namanya Presto) atau Spark SQL:** Sebagai jalan pintas nge-query data pakai SQL biasa langsung menembus HDFS.

---

## Matriks Perbandingan 

| Dimensi Arsitektur | Sebelum  | Sesudah  | Status Upgrade |
|---|---|---|---|
| **Format File** | Mentah (`.json`) | Parquet + Metadata Hudi (Columnar & Padat) | 🚀 Jauh lebih hemat storage |
| **Keandalan Data** | ❌ Tidak ada jaminan ACID, rawan corrupt | ✅ Transaksi ACID via Hudi CoW/MoR | 🛡️ Sangat aman |
| **Aturan Skema** | ❌ Bebas liar, nulis apa aja masuk | ✅ Terikat ketat di Hive Metastore | 📏 Terpola rapi |
| **Deduplikasi** | Hanya sekali di level producer | Double filter (Producer + Silver Layer Upsert) | 🧹 Data bersih tanpa ganda |
| **Sejarah Data** | ❌ Hilang ditimpa data baru | ✅ Ada fitur Time Travel & Rollback | ⏳ Bisa intip masa lalu |
| **Kecepatan Query** | 🔴 Lambat  | 🟢 Cepat | ⚡ Auto-cepat |
| **Alur Pemrosesan** | Langsung taruh HDFS tanpa proses | Terbagi rapi: Bronze ➡️ Silver ➡️ Gold | 🏗️ Sangat terstruktur |
| **Kompleksitas** | 🟢 Enteng, setup cuma 5 menit | 🔴 Lumayan butuh tenaga (konfigurasi Spark) | 🧠 Menantang tapi keren |

---

## 🗺️ Rencana Migrasi

Kami matikan pipeline yang sekarang jalan

```mermaid
gantt
    title Roadmap Migrasi GempaRadar ke Lakehouse (Target: 1 Bulan)
    dateFormat  YYYY-MM-DD
    axisFormat  %d-%m

    section Fase 1 · Fondasi
    Setup Spark + Hudi di Docker           :a1, 2026-06-01, 7d
    Integrasi Hive Metastore               :a2, after a1, 5d

    section Fase 2 · Bronze Layer
    Migrasi Consumer ke Spark Streaming    :b1, after a2, 7d
    Pipe data Kafka ke Bronze (Hudi Table) :b2, after b1, 3d

    section Fase 3 · Silver Layer
    Bikin ETL Job Bronze ke Silver         :c1, after b2, 7d
    Setup skema ketat & filter data kotor  :c2, after c1, 5d

    section Fase 4 · Gold & Serving
    Bikin Ringkasan Agregat (Gold Layer)   :d1, after c2, 7d
    Ganti Query Flask ke Trino/Spark SQL   :e1, after d1, 7d
```

---

### 🥉 1. Menulis ke Bronze Layer (Dari Stream Kafka ke Hudi CoW)

Script consumer lama kita ganti pake Spark Streaming:

```python
# consumer_to_bronze.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

# Inisialisasi Spark Session lengkap dengan konfigurasi Hudi
spark = SparkSession.builder \
    .appName("GempaRadar-KafkaToBronze") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .getOrCreate()

# Baca stream data langsung dari Kafka Broker
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "gempa-api") \
    .load()

# Ubah value Kafka yang tadinya biner jadi string biasa
raw_json_df = kafka_stream.selectExpr("CAST(value AS STRING) as json_payload")

# Konfigurasi Hudi untuk nulis ke HDFS Bronze Layer
hudi_options = {
    "hoodie.table.name": "gempa_api_bronze",
    "hoodie.datasource.write.recordkey.field": "id",            # Field unik buat kunci data
    "hoodie.datasource.write.precombine.field": "timestamp",    # Buat milih data terbaru kalau ada duplikat
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "insert"
}

# Mulai jalankan streaming engine-nya!
query = raw_json_df.writeStream \
    .format("hudi") \
    .options(**hudi_options) \
    .option("checkpointLocation", "/lakehouse/checkpoints/gempa_api_bronze") \
    .mode("append") \
    .start("/lakehouse/bronze/gempa_api/")

query.awaitTermination()
```

### 🥈 2. Silver Layer (Pembersihan & Pengetatan Skema)

Setelah data di bronze dibersihkan dari nilai-nilai null:

```python
# etl_bronze_to_silver.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("GempaRadar-BronzeToSilver") \
    .getOrCreate()

# Baca data tabel Hudi dari Bronze Layer
bronze_df = spark.read.format("hudi").load("/lakehouse/bronze/gempa_api/")

# Waktunya bersih-bersih!
cleaned_df = bronze_df \
    .dropDuplicates(["id"]) \
    .filter(col("magnitude").isNotNull() & (col("magnitude") > 0.0)) \
    .withColumn("depth_km", col("depth").cast("double")) \
    .withColumn("event_time", to_timestamp(col("time_epoch") / 1000))

# Upsert (Update jika ada yang berubah, Insert jika data baru) ke Silver
hudi_upsert_options = {
    "hoodie.table.name": "gempa_events_silver",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "event_time",
    "hoodie.datasource.write.operation": "upsert"               # Kekuatan utama Hudi: bisa update!
}

cleaned_df.write \
    .format("hudi") \
    .options(**hudi_upsert_options) \
    .mode("append") \
    .save("/lakehouse/silver/gempa_events/")
```

---

## 📁 Struktur Folder di HDFS Baru Kita

Nanti kalau kita SSH ke NameNode HDFS dan ketik `hdfs dfs -ls /lakehouse`, tampilannya akan seperti ini:

```
/lakehouse/
├── bronze/
│   ├── gempa_api/          ← Raw event USGS (Format Parquet + Metadata Hudi)
│   └── gempa_rss/          ← Raw berita BMKG/Media (Format Parquet + Metadata Hudi)
├── silver/
│   ├── gempa_events/       ← Data gempa bersih, udah rapi ter-skema
│   └── berita_gempa/       ← Berita bersih yang tag HTML-nya udah dibuang
└── gold/
    ├── gempa_daily_stats/  ← Tabel agregat statistik gempa harian (Dashboard read ini!)
    ├── hotspot_wilayah/    ← Hasil klasterisasi wilayah rawan gempa
    └── korelasi_berita/    ← Tabel joinan antara data kejadian gempa dengan berita media
```

---
