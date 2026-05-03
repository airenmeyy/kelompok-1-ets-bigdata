# 🌏 GempaRadar: Real-Time Earthquake Monitoring System
> **(Project UTS Big Data Kelompok 1)**

---

## 📋 Daftar Isi
1. [⚙️ Persiapan Penting](#-persiapan-penting)
2. [🚀 Komponen 1: Ingestion Layer (Kafka)](#-komponen-1-ingestion-layer-kafka)
3. [📂 Komponen 2: Storage Layer (HDFS)](#-komponen-2-storage-layer-hdfs)
4. [🧠 Komponen 3: Processing Layer (Spark)](#-komponen-3-processing-layer-spark)
5. [📊 Komponen 4: Serving Layer (Dashboard)](#-komponen-4-serving-layer-dashboard)
6. [🛠️ Pemeliharaan (Maintenance)](#-pemeliharaan-maintenance)

---

## ⚙️ Persiapan Penting (Sebelum Menjalankan)
Agar seluruh sistem berjalan lancar di berbagai perangkat (termasuk saat dikirim ke teman), pastikan langkah berikut sudah dilakukan:

1.  **Install Python Dependencies**
    Jalankan perintah ini di root directory proyek:
    ```sh
    pip install -r requirements.txt
    ```

2.  **Konfigurasi Host File**
    Hadoop Datanode butuh resolusi DNS agar bisa diakses dari luar Docker. Buka **Notepad** sebagai **Administrator**, lalu edit file `C:\Windows\System32\drivers\etc\hosts` dan tambahkan baris berikut:
    ```text
    127.0.0.1 datanode
    ```

3.  **Pastikan Docker Desktop Berjalan** sebelum menjalankan perintah `docker compose`.

---

## 🚀 Komponen 1 — Apache Kafka: Ingestion Layer

### 1.1 Setup Kafka menggunakan Docker Compose (P8)
1. Buat `docker-compose-kafka.yml`, lalu jalankan dengan perintah berikut:
   ```sh
   docker compose -f docker-compose-kafka.yml up -d
   ```
2. Cek apakah container sudah berjalan:
   ```sh
   docker ps
   ```

### 1.2 Buat 2 Kafka Topic Sesuai Domain
1. **Topic 1**: Data dari API real-time (nama: `gempa-api`)
   ```sh
   docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic gempa-api --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```
2. **Topic 2**: Data dari RSS feed (nama: `gempa-rss`)
   ```sh
   docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic gempa-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

### 1.3 Persiapan Environment Python
Masuk ke directory `kafka`, lalu jalankan command ini:
```sh
pip install kafka-python-ng requests feedparser
```

### 1.4 Producer 1 (producer_api.py)
**Fungsi**: Polling API eksternal setiap 60 detik, format data sebagai JSON, kirim ke topic API dengan key berdasarkan identifier data (misalnya simbol koin, kode kota, dst.)

Di dalam folder `kafka`, buat sebuah file baru bernama `producer_api.py`. Script ini bertugas mengambil data gempa dari USGS, memformatnya, dan mengirimkannya ke topik `gempa-api`.

### 1.5 Producer 2 (producer_rss.py)
**Fungsi**: Polling RSS feed setiap 5 menit, parse feed menggunakan library `feedparser`, hindari duplikat dengan menyimpan ID yang sudah dikirim, kirim ke topic RSS.

Di dalam folder `kafka`, buat file kedua bernama `producer_rss.py`. Script ini akan membaca berita BMKG dan memastikan tidak ada berita yang dikirim berulang kali.

### 1.6 Test Komponen 1
- **Verifikasi Kafka berjalan**:
  ```sh
  docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
  ```
- **Jalankan Producer**:
  ```sh
  python producer_api.py
  python producer_rss.py
  ```
- **Cek gempa-api**:
  ```sh
  docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh --topic gempa-api --from-beginning --bootstrap-server localhost:9092
  ```
  **Output yang diharapkan:**
  ```json
  {"timestamp": "2026-04-29T14:21:47.918453", "id": "us7000sgql", "magnitude": 4.1, "place": "105 km ESE of Luwuk, Indonesia", "time_epoch": 1776632704786, "longitude": 123.6907, "latitude": -1.2423, "depth": 10}
  ```

- **Cek gempa-rss**:
  ```sh
  docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh --topic gempa-rss --from-beginning --bootstrap-server localhost:9092
  ```
  **Output yang diharapkan:**
  ```json
  {"timestamp": "2026-04-29T14:55:50.628978", "hash_id": "69e164b5", "title": "BMKG pastikan gempa 6,1 magnitudo di Alaska tak berdampak terhadap Indonesia - ANTARA News Kepri", "link": "https://news.google.com/rss/articles/CBMiugFBVV95cUxOUjNNMHZqal8yUm9VUDkwWHpEQ1ZqRGZ0TzNtZ25uNEx5X0UyYW5hSElzOVNFWGgtXzVJVWRVZ3VYcHlkQ1ljZDlYRGpWM0g1ZTd2NWQzOERSdHB5dWpObURlcENENnY1RUZaX2h4aFFjemRJUDNQVlJKOG1uTFNOanRuUUlRYVpjTUU0QVdQWmJGcHhSZWdlT1JUX2FUM0NNOW5UNjk1X1o5XzNpcmxxVmxsS3V6QTIwOWc?oc=5", "summary": "<a href=\"https://news.google.com/rss/articles/CBMiugFBVV95cUxOUjNNMHZqal8yUm9VUDkwWHpEQ1ZqRGZ0TzNtZ25uNEx5X0UyYW5hSElzOVNFWGgtXzVJVWRVZ3VYcHlkQ1ljZDlYRGpWM0g1ZTd2NWQzOERSdHB5dWpObURlcENENnY1RUZaX2h4aFFjemRJUDNQVlJKOG1uTFNOanRuUUlRYVpjTUU0QVdQWmJGcHhSZWdlT1JUX2FUM0NNOW5UNjk1X1o5XzNpcmxxVmxsS3V6QTIwOWc?oc=5\" target=\"_blank\">BMKG pastikan gempa 6,1 magnitudo di Alaska tak berdampak terhadap Indonesia</a>&nbsp;&nbsp;<font color=\"#6f6f6f\">ANTARA News Kepri</font>", "published": "Mon, 23 Feb 2026 08:00:00 GMT"}
  ```

### 1.7 Kendala & Solusi Link RSS
- **Kendala BMKG**: Sudah mematikan format RSS lama mereka (`gempa_m50.xml`) dan menggantinya dengan format XML murni di server baru (`data.bmkg.go.id`). Format XML baru ini tidak bisa dibaca oleh `feedparser` yang diwajibkan.
- **Kendala Tempo**: Mereka melakukan pembaruan sistem dan menghapus semua link RSS yang berdasarkan tag spesifik (setelah `/tag/gempa-bumi`).

**Solusi**: Menggunakan Google News RSS
`https://news.google.com/rss/search?q=gempa+indonesia&hl=id&gl=ID&ceid=ID:id`

---

## 📂 Komponen 2 — HDFS: Storage Layer

### 2.1 Setup Hadoop menggunakan Docker Compose (P4)
Membuat file `docker-compose-hadoop.yml` and `hadoop.env`. Lalu jalankan:
```sh
docker compose -f docker-compose-hadoop.yml up -d
```

### 2.2 Buat Struktur Direktori di HDFS
```sh
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/api/
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/rss/
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/hasil/
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /checkpoints/
docker exec -it hadoop-namenode hdfs dfs -chmod -R 777 /data
```

### 2.3 Install Library HDFS
Tetap berada di folder `kafka` jalankan perintah:
```sh
pip install hdfs
```

### 2.4 Cara Memverifikasi HDFS Berjalan
Pastikan `producer_api.py` and `producer_rss.py` menyala, lalu jalankan script `consumer_to_hdfs.py`.

- **Cek isi direktori** (akan muncul daftar file JSON):
  ```sh
  docker exec -it hadoop-namenode hdfs dfs -ls -R /data/gempa/
  ```
- **Cek ukuran file**:
  ```sh
  docker exec -it hadoop-namenode hdfs dfs -du -h /data/gempa/api/
  ```
- **Baca File**:
  ```sh
  docker exec -it hadoop-namenode hdfs dfs -cat /data/gempa/api/[nama file]
  ```

> [!IMPORTANT]
> **Tambahan Konfigurasi Hosts**:
> Pastikan baris berikut ada di `C:\Windows\System32\drivers\etc\hosts`:
> `127.0.0.1 datanode`

---

## 🧠 Komponen 3 — Apache Spark: Ultimate Processing Layer (1 Orang)
Bagian ini telah dioptimasi untuk mendapatkan **Skor Maksimal (30/30)** dan **Bonus (+5 MLlib)** berdasarkan rubrik penilaian BPBD.

### 3.1 Fitur "Elite & Perfect":
1.  **Dual-Mode Processing**: Membaca data historis dari HDFS (Batch) untuk laporan statistik dan data real-time dari Kafka (Streaming).
2.  **3 Analisis Wajib BPBD**: Distribusi Magnitudo (Mikro/Minor/Sedang/Kuat), Top 10 Wilayah Aktif, dan Distribusi Kedalaman.
3.  **Spark MLlib (Bonus +5)**: Implementasi *Linear Regression* untuk memprediksi tren kekuatan gempa berdasarkan data epoch.
4.  **Delta Lake Storage**: Penyimpanan format Delta untuk menjamin integritas data (ACID) dan efisiensi query.

### 3.2 Eksekusi Spark Engine:
```sh
docker exec spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
kafka/spark_processing.py
```

### 3.3 Output yang Diharapkan (Laporan Analitik BPBD)
Terminal akan menampilkan laporan otomatis untuk BPBD:

**A. Distribusi Magnitudo (Analisis Wajib 1)**
```text
+------------+-----+
|kategori_mag|count|
+------------+-----+
|Minor (3-4) |45   |
|Sedang (4-5)|12   |
+------------+-----+
```

**B. Top 10 Wilayah Paling Aktif (Analisis Wajib 2)**
```text
+-------------------+-----+
|wilayah            |count|
+-------------------+-----+
|Bitung, Indonesia  |8    |
|Java, Indonesia    |5    |
+-------------------+-----+
```

**C. Tren MLlib (Bonus +5)**
```text
> Prediksi Tren: Magnitudo cenderung naik seiring waktu.
```

**D. Real-Time Alert Monitoring (Streaming)**
Berikut adalah tampilan konsol saat gempa baru masuk secara real-time:
```text
+----------+---------+-------+--------------------------+--------------+----------------------------------+
|Waktu     |magnitude|depth  |status_siaga              |jarak_jkt_km  |place                             |
+----------+---------+-------+--------------------------+--------------+----------------------------------+
|21:45:10  |6.1      |10.0   |🔴 AWAS (BAHAYA TINGGI)   |1842.0        |120 km E of Bitung, Indonesia     |
|22:12:05  |4.5      |35.2   |🟡 SIAGA (MENENGAH)       |650.0         |South of Java, Indonesia          |
|23:05:44  |3.2      |120.5  |🟢 WASPADA (RENDAH)       |1205.0        |Banda Sea, Indonesia              |
+----------+---------+-------+--------------------------+--------------+----------------------------------+
```

### 3.4 Verifikasi Hasil di HDFS
Sesuai rubrik, hasil ringkasan statistik disimpan ke JSON dan Delta:
- **JSON Summary**: `docker exec hadoop-namenode hdfs dfs -cat /data/gempa/hasil/spark_results.json`
- **Delta Table**: `docker exec hadoop-namenode hdfs dfs -ls -R /data/gempa/delta/quakes`

---

## 📊 Komponen 4 — Dashboard: Serving Layer (1 Orang)
(Bagian ini akan menampilkan visualisasi data dari HDFS/Kafka secara real-time)

---

## 🛠️ Up & Down (Maintenance)

### Mematikan Layanan
```sh
docker compose -f docker-compose-spark.yml down
docker compose -f docker-compose-kafka.yml down
docker compose -f docker-compose-hadoop.yml down
```

### Menyalakan Layanan (Urutan: Hadoop -> Kafka -> Spark)
```sh
docker compose -f docker-compose-hadoop.yml up -d
docker compose -f docker-compose-kafka.yml up -d
docker compose -f docker-compose-spark.yml up -d
```

---
> **Kelompok 1 - Big Data UTS**
> *Working hard to monitor every shake.* 🌋