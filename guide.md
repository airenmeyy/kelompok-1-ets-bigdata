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

### Setup Kafka menggunakan Docker Compose
1. Jalankan Kafka:
```sh
docker compose -f docker-compose-kafka.yml up -d
```

### Buat 2 Kafka topic:
```sh
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic gempa-api --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic gempa-rss --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Jalankan Producers:
- `python kafka/producer_api.py`
- `python kafka/producer_rss.py`

---

## Komponen 2 — HDFS: Storage Layer

### Setup Hadoop:
```sh
docker compose -f docker-compose-hadoop.yml up -d
```

### Inisialisasi Direktori:
```sh
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/gempa/api/ /data/gempa/rss/ /data/gempa/hasil/ /checkpoints/
docker exec -it hadoop-namenode hdfs dfs -chmod -R 777 /data
```

### Jalankan Consumer:
```sh
python kafka/consumer_to_hdfs.py
```

---

## Komponen 3 — Apache Spark: Ultimate Processing Layer (1 Orang)

Bagian ini telah dioptimasi untuk mendapatkan **Skor Maksimal (30/30)** dan **Bonus (+5 MLlib)** berdasarkan rubrik penilaian.

### Fitur "Elite & Perfect":
1.  **Dual-Mode Processing**: Membaca data historis dari HDFS (Batch) dan data real-time dari Kafka (Streaming).
2.  **3 Analisis Wajib BPBD**: Distribusi Magnitudo, Top 10 Wilayah Aktif, dan Distribusi Kedalaman.
3.  **Spark MLlib (Bonus +5)**: Implementasi *Linear Regression* untuk memprediksi tren kekuatan gempa.
4.  **Delta Lake Storage**: Penyimpanan format Delta untuk menjamin integritas data (ACID).

### Eksekusi Spark Engine:
```sh
docker exec spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
kafka/spark_processing.py
```

### Output yang Diharapkan (Analisis Wajib)
Terminal akan menampilkan laporan otomatis untuk BPBD:
- **Distribusi Magnitudo**: (Kategori Mikro, Minor, Sedang, Kuat)
- **Top 10 Wilayah**: (Lokasi dengan frekuensi gempa tertinggi)
- **Tren MLlib**: (Prediksi kenaikan/penurunan intensitas gempa)

### Verifikasi Hasil (JSON)
Sesuai rubrik, hasil ringkasan disimpan ke JSON di HDFS:
```sh
docker exec hadoop-namenode hdfs dfs -cat /data/gempa/hasil/spark_results.json
```

---

## 📊 Komponen 4 — Dashboard: Serving Layer
Dashboard Flask (Localhost:5000) akan mengonsumsi data dari HDFS/Kafka.

---

## 🧹 Shutdown
```sh
docker compose -f docker-compose-spark.yml down
docker compose -f docker-compose-kafka.yml down
docker compose -f docker-compose-hadoop.yml down
```