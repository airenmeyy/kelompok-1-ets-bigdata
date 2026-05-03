# 🌏 GempaRadar: Real-Time Earthquake Monitoring System
> **Project UTS Big Data - Kelompok 1**
> Sistem monitoring gempa bumi terintegrasi menggunakan ekosistem Apache (Kafka, Hadoop, Spark).

---

## 📋 Daftar Isi
1. [⚙️ Persiapan Penting](#-persiapan-penting)
2. [🚀 Komponen 1: Ingestion Layer (Kafka)](#-komponen-1-ingestion-layer-kafka)
3. [📂 Komponen 2: Storage Layer (HDFS)](#-komponen-2-storage-layer-hdfs)
4. [🧠 Komponen 3: Processing Layer (Spark)](#-komponen-3-processing-layer-spark)
5. [📊 Komponen 4: Serving Layer (Dashboard)](#-komponen-4-serving-layer-dashboard)
6. [🛠️ Pemeliharaan (Maintenance)](#️-pemeliharaan-maintenance)

---

## ⚙️ Persiapan Penting
Sebelum menjalankan sistem, pastikan langkah-langkah berikut sudah dilakukan agar tidak terjadi error koneksi:

1.  **Install Python Dependencies**
    Jalankan perintah ini di root directory proyek:
    ```sh
    pip install -r requirements.txt
    ```

2.  **Konfigurasi DNS (Host File)**
    Hadoop Datanode membutuhkan resolusi DNS agar bisa diakses dari luar Docker.
    - Buka **Notepad** sebagai **Administrator**.
    - Edit file: `C:\Windows\System32\drivers\etc\hosts`
    - Tambahkan baris berikut di bagian paling bawah:
      ```text
      127.0.0.1 datanode
      ```

3.  **Cek Docker Desktop**
    Pastikan Docker Desktop sudah running sebelum mengeksekusi file `yml`.

---

## 🚀 Komponen 1: Ingestion Layer (Kafka)
Layer ini berfungsi untuk mengambil data dari sumber eksternal (API & RSS) dan memasukkannya ke dalam antrian Kafka.

### 1.1 Inisialisasi Broker
Jalankan Kafka broker menggunakan Docker:
```sh
docker compose -f docker-compose-kafka.yml up -d
```

### 1.2 Pembuatan Topic
Buat dua topic utama untuk memisahkan aliran data:
- **Topic API**: `gempa-api`
- **Topic RSS**: `gempa-rss`

```sh
# Buat Topic API
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic gempa-api --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Buat Topic RSS
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic gempa-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 1.3 Menjalankan Producer
Masuk ke direktori `kafka/` dan jalankan script pengambil data:

- **Producer API**: Polling USGS API setiap 60 detik (Output: JSON).
  ```sh
  python kafka/producer_api.py
  ```
- **Producer RSS**: Polling Google News RSS setiap 5 menit.
  ```sh
  python kafka/producer_rss.py
  ```

> [!TIP]
> **Mengapa Google News?** 
> Kami menggunakan Google News RSS karena RSS BMKG sedang dalam pemeliharaan (migrasi ke format XML murni yang tidak kompatibel dengan `feedparser` standar).

---

## 📂 Komponen 2: Storage Layer (HDFS)
Data yang masuk ke Kafka akan disimpan secara permanen di Hadoop Distributed File System.

### 2.1 Inisialisasi Hadoop
Jalankan cluster Hadoop:
```sh
docker compose -f docker-compose-hadoop.yml up -d
```

### 2.2 Konfigurasi Struktur Direktori
Siapkan folder untuk penyimpanan data mentah dan hasil olahan:
```sh
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/api/
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/rss/
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/hasil/
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /checkpoints/
docker exec -it hadoop-namenode hdfs dfs -chmod -R 777 /data
```

### 2.3 Verifikasi Penyimpanan
Untuk melihat apakah data sudah masuk ke HDFS:
```sh
# Cek list file
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/gempa/

# Cek ukuran data
docker exec -it hadoop-namenode hdfs dfs -du -h /data/gempa/api/
```

---

## 🧠 Komponen 3: Processing Layer (Spark)
Data diproses menggunakan Spark Structured Streaming untuk analisis real-time dan MLlib.

### 3.1 Fitur Unggulan
- ✅ **Dual-Mode**: Batch (HDFS) + Streaming (Kafka).
- ✅ **BPBD Compliance**: Analisis distribusi magnitudo, kedalaman, dan wilayah aktif.
- ✅ **MLlib Prediction**: Prediksi tren kekuatan gempa menggunakan *Linear Regression*.
- ✅ **Delta Lake**: Menjamin integritas data dengan ACID transactions.

### 3.2 Menjalankan Spark Engine
Gunakan `spark-submit` untuk menjalankan pipeline pemrosesan:
```sh
docker exec spark-master /opt/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
kafka/spark_processing.py
```

### 3.3 Output Laporan Analitik
Spark akan menampilkan tabel ringkasan seperti berikut di konsol:
```text
+----------+---------+-------+--------------------------+--------------+
|Waktu     |magnitude|depth  |status_siaga              |jarak_jkt_km  |
+----------+---------+-------+--------------------------+--------------+
|21:45:10  |6.1      |10.0   |🔴 AWAS (BAHAYA TINGGI)   |1842.0        |
|22:12:05  |4.5      |35.2   |🟡 SIAGA (MENENGAH)       |650.0         |
+----------+---------+-------+--------------------------+--------------+
```

---

## 📊 Komponen 4: Serving Layer (Dashboard)
*(Dalam Pengembangan)*
Bagian ini akan memvisualisasikan data dari HDFS/Delta Lake ke dalam dashboard interaktif untuk memudahkan pengambilan keputusan oleh pihak terkait.

---

## 🛠️ Pemeliharaan (Maintenance)

### Mematikan Seluruh Layanan
```sh
docker compose -f docker-compose-spark.yml down
docker compose -f docker-compose-kafka.yml down
docker compose -f docker-compose-hadoop.yml down
```

### Menjalankan Ulang (Urutan yang Benar)
Pastikan urutan ini diikuti agar dependensi antar layanan terpenuhi:
1.  **Hadoop** (Storage)
2.  **Kafka** (Ingestion)
3.  **Spark** (Processing)

```sh
docker compose -f docker-compose-hadoop.yml up -d && \
docker compose -f docker-compose-kafka.yml up -d && \
docker compose -f docker-compose-spark.yml up -d
```

---
> **Kelompok 1 - Big Data UTS**
> *Working hard to monitor every shake.* 🌋