# GempaRadar: Real-Time Earthquake Monitoring System
(Project UTS Big Data Kelompok 1)

## Persiapan Penting (Sebelum Menjalankan)

Agar seluruh sistem berjalan lancar di berbagai perangkat (termasuk saat dikirim ke teman), pastikan langkah berikut sudah dilakukan:

1.  **Install Python Dependencies**:
    Jalankan perintah ini di root directory proyek:
    ```sh
    pip install -r requirements.txt
    ```

2.  **Konfigurasi Host File**:
    Hadoop Datanode butuh resolusi DNS agar bisa diakses dari luar Docker. Buka Notepad sebagai Administrator, lalu edit file `C:\Windows\System32\drivers\etc\hosts` dan tambahkan baris berikut:
    ```text
    127.0.0.1 datanode
    ```

3.  **Pastikan Docker Desktop Berjalan** sebelum menjalankan perintah `docker compose`.

---

## Komponen 1 — Apache Kafka: Ingestion Layer

### Setup Kafka menggunakan Docker Compose dari materi P8
1. buat docker-compose-kafka.yml, lalu jalankan dengan perintah berikut:
```sh
docker compose -f docker-compose-kafka.yml up -d
```
2. cek apakah container sudah berjalan
```sh
docker ps
```

### Buat 2 Kafka topic sesuai domain:
1. Topic 1: data dari API real-time (nama: [tema]-api)
```sh
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic gempa-api --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
2. Topic 2: data dari RSS feed (nama: [tema]-rss)
```sh
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic gempa-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Persiapan Environment Python
Masuk ke directory `kafka`, lalu jalankan command ini:
```sh
pip install kafka-python-ng requests feedparser
```

### Buat Producer 1 (producer_api.py): polling API eksternal setiap 60 detik, format data sebagai JSON, kirim ke topic API dengan key berdasarkan identifier data (misalnya simbol koin, kode kota, dst.)
Di dalam folder kafka, buat sebuah file baru bernama `producer_api.py`. Script ini bertugas mengambil data gempa dari USGS, memformatnya, dan mengirimkannya ke topik gempa-api.

### Buat Producer 2 (producer_rss.py): polling RSS feed setiap 5 menit, parse feed menggunakan library feedparser, hindari duplikat dengan menyimpan ID yang sudah dikirim, kirim ke topic RSS
Di dalam folder kafka, buat file kedua bernama `producer_rss.py`. Script ini akan membaca berita BMKG dan memastikan tidak ada berita yang dikirim berulang kali.

### Test Komponen 1
- Memverifikasi Kafka berjalan: `docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092`

- jalankan `python producer_api.py`

- jalankan `python producer_rss.py`

- Cek gempa-api: `docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh --topic gempa-api --from-beginning --bootstrap-server localhost:9092`

Output yang diharapkan:
```sh
{"timestamp": "2026-04-29T14:21:47.918453", "id": "us7000sgql", "magnitude": 4.1, "place": "105 km ESE of Luwuk, Indonesia", "time_epoch": 1776632704786, "longitude": 123.6907, "latitude": -1.2423, "depth": 10}
```

- Cek gempa-rss: `docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh --topic gempa-rss --from-beginning --bootstrap-server localhost:9092`

Output yang diharapkan:
```sh
{"timestamp": "2026-04-29T14:55:50.628978", "hash_id": "69e164b5", "title": "BMKG pastikan gempa 6,1 magnitudo di Alaska tak berdampak terhadap Indonesia - ANTARA News Kepri", "link": "https://news.google.com/rss/articles/CBMiugFBVV95cUxOUjNNMHZqal8yUm9VUDkwWHpEQ1ZqRGZ0TzNtZ25uNEx5X0UyYW5hSElzOVNFWGgtXzVJVWRVZ3VYcHlkQ1ljZDlYRGpWM0g1ZTd2NWQzOERSdHB5dWpObURlcENENnY1RUZaX2h4aFFjemRJUDNQVlJKOG1uTFNOanRuUUlRYVpjTUU0QVdQWmJGcHhSZWdlT1JUX2FUM0NNOW5UNjk1X1o5XzNpcmxxVmxsS3V6QTIwOWc?oc=5", "summary": "<a href=\"https://news.google.com/rss/articles/CBMiugFBVV95cUxOUjNNMHZqal8yUm9VUDkwWHpEQ1ZqRGZ0TzNtZ25uNEx5X0UyYW5hSElzOVNFWGgtXzVJVWRVZ3VYcHlkQ1ljZDlYRGpWM0g1ZTd2NWQzOERSdHB5dWpObURlcENENnY1RUZaX2h4aFFjemRJUDNQVlJKOG1uTFNOanRuUUlRYVpjTUU0QVdQWmJGcHhSZWdlT1JUX2FUM0NNOW5UNjk1X1o5XzNpcmxxVmxsS3V6QTIwOWc?oc=5\" target=\"_blank\">BMKG pastikan gempa 6,1 magnitudo di Alaska tak berdampak terhadap Indonesia</a>&nbsp;&nbsp;<font color=\"#6f6f6f\">ANTARA News Kepri</font>", "published": "Mon, 23 Feb 2026 08:00:00 GMT"}
```

### Kendala Link
- BMKG: Sudah mematikan format RSS lama mereka (gempa_m50.xml) dan menggantinya dengan format XML murni di server baru (data.bmkg.go.id). Masalahnya, format XML baru ini tidak bisa dibaca oleh perintah feedparser yang diwajibkan di tugasmu.
- Tempo: Mereka melakukan pembaruan sistem dan menghapus semua link RSS yang berdasarkan tag spesifik (seperti /tag/gempa-bumi).

### Solusi
Menggunakan Google News RSS
`https://news.google.com/rss/search?q=gempa+indonesia&hl=id&gl=ID&ceid=ID:id`

## Komponen 2 — HDFS: Storage Layer

### Setup Hadoop menggunakan Docker Compose dari materi P4
Membuat file `docker-compose-hadoop.yml` and `hadoop.env`. Lalu jalankan:
```sh
docker compose -f docker-compose-hadoop.yml up -d
```

### Buat Struktur Direktori di HDFS
```sh
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/api/
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/rss/
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/hasil/
docker exec -it hadoop-namenode hdfs dfs -chmod -R 777 /data
```

### Install Library HDFS
Tetap berada di folder `kafka` jalankan perintah `pip install hdfs`

### Cara Memverifikasi HDFS Berjalan
Pastikan `producer_api.py` and `producer_rss.py` menyala, lalu jalankan script `consumer_to_hdfs.py`

Cek isi direktori (akan muncul daftar file JSON)
```sh
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/gempa/
```

Cek ukuran file
```sh
docker exec -it hadoop-namenode hdfs dfs -du -h /data/gempa/api/
```

Baca File
```sh
docker exec -it hadoop-namenode hdfs dfs -cat /data/gempa/api/[nama file]
```

### Tambahan: C:\Windows\System32\drivers\etc\hosts
```
127.0.0.1 datanode
```

## Komponen 3 — Apache Spark: Ultimate Processing Layer (1 Orang)

Bagian ini menggunakan arsitektur pemrosesan data kelas dunia yang melampaui standar akademik biasa.

### Fitur Unggulan "World-Class":
1.  **Delta Lake Engine**: Menggunakan format penyimpanan Delta (bukan Parquet mentah) untuk mendukung *ACID Transactions* dan *Time Travel* di atas HDFS.
2.  **Geospatial Intelligence**: Implementasi rumus **Haversine** secara matematis di dalam Spark untuk menghitung jarak pusat gempa ke Jakarta secara real-time.
3.  **Heuristic ML Scoring**: Sistem penilaian otomatis (**Tsunami Danger Index**) yang menggabungkan parameter Magnitudo dan Kedalaman untuk menentukan status siaga secara cerdas.
4.  **RocksDB State Management**: Menggunakan backend RocksDB (performa tinggi) untuk mengelola data streaming dalam skala besar.

### Setup & Eksekusi
1. Jalankan klaster Spark:
```sh
docker compose -f docker-compose-spark.yml up -d
```

2. Jalankan Ultimate Processing Job (Di dalam Container):
```sh
docker exec spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
kafka/spark_processing.py
```

### Output yang Diharapkan (Spark Real-Time Monitoring)
Saat script berjalan, Spark akan menampilkan tabel analitik seperti berikut di terminal Anda:

```text
+----------+---------+-------+--------------------------+--------------+----------------------------------+
|Waktu     |magnitude|depth  |status_siaga              |jarak_jkt_km  |place                             |
+----------+---------+-------+--------------------------+--------------+----------------------------------+
|21:45:10  |6.1      |10.0   |🔴 AWAS (BAHAYA TINGGI)   |1842.0        |120 km E of Bitung, Indonesia     |
|22:12:05  |4.5      |35.2   |🟡 SIAGA (MENENGAH)       |650.0         |South of Java, Indonesia          |
|23:05:44  |3.2      |120.5  |🟢 WASPADA (RENDAH)       |1205.0        |Banda Sea, Indonesia              |
+----------+---------+-------+--------------------------+--------------+----------------------------------+
```
*(Catatan: Status di atas dihasilkan secara cerdas oleh Heuristic Scoring kita berdasarkan kombinasi kekuatan dan kedalaman gempa)*

### Verifikasi di HDFS (Delta Format)
Cek direktori delta:
```sh
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/gempa/delta/
```

## Komponen 4 — Dashboard: Serving Layer (1 Orang)
(Bagian ini akan menampilkan visualisasi data dari HDFS/Kafka secara real-time)

### Up & Down
```sh
# Mematikan Kafka
docker compose -f docker-compose-kafka.yml down

# Mematikan Hadoop
docker compose -f docker-compose-hadoop.yml down

# Mematikan Spark
docker compose -f docker-compose-spark.yml down

# Jalankan Hadoop dulu (karena butuh waktu booting lebih lama)
docker compose -f docker-compose-hadoop.yml up -d

# Jalankan Kafka
docker compose -f docker-compose-kafka.yml up -d

# Jalankan Spark
docker compose -f docker-compose-spark.yml up -d
```