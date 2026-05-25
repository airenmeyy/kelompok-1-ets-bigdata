# Setup Spark + Delta Lake untuk GempaRadar Lakehouse

Dokumen ini berisi panduan untuk melakukan setup environment dan menjalankan pipeline data **Bronze Layer** pada sistem GempaRadar. Panduan ini ditujukan bagi seluruh anggota Kelompok 1 ETS Big Data.

---

## 📌 Prasyarat Sebelum Mulai

Pastikan komponen-komponen berikut sudah terpasang dan berjalan di komputer masing-masing:

1. **Docker Desktop** sudah terinstall dan sedang berjalan.
2. **Python 3.10+** dengan virtual environment (`.venv`) yang sudah dibuat dan aktif.
3. Memastikan package `delta-spark` versi `3.1.0` sudah terinstall di `.venv` lokal:
   ```bash
   pip install delta-spark==3.1.0
   ```
   *Catatan: Package ini sudah dideklarasikan di `requirements.txt` proyek.*

---

## 🏗️ Cara Menjalankan Infrastruktur (Urutan Wajib)

Jalankan container Docker untuk masing-masing layanan dengan urutan sebagai berikut dari root project (`kelompok-1-ets-bigdata/`):

```bash
# 1. Jalankan layanan Hadoop HDFS terlebih dahulu
docker compose -f docker-compose-hadoop.yml up -d

# 1.5. (KHUSUS PERTAMA KALI SETUP) Siapkan permission direktori HDFS untuk Spark (menghindari AccessControlException)
docker exec hadoop-namenode bash -c "hdfs dfs -mkdir -p /user/root && hdfs dfs -chmod -R 777 /user"

# 2. Jalankan Apache Kafka
docker compose -f docker-compose-kafka.yml up -d

# 3. Jalankan Apache Spark (Master & Worker)
docker compose -f docker-compose-spark.yml up -d
```

### Verifikasi Container Aktif
Untuk memastikan semua container aktif dan berjalan lancar, jalankan perintah:
```bash
docker ps
```
Pastikan container berikut berstatus *Up*:
* `hadoop-namenode` (HDFS NameNode)
* `kafka-broker` (Kafka Broker)
* `spark-master` (Spark Master)
* `spark-worker` (Spark Worker)

---

## 🥉 Cara Menjalankan Bronze Layer

Pipeline Bronze Layer dapat dieksekusi langsung menggunakan python script. Pastikan terminal berada di root project (`kelompok-1-ets-bigdata/`):

```bash
# Pindah ke root folder project jika belum
cd kelompok-1-ets-bigdata

# Jalankan script Bronze Layer
python lakehouse/01_bronze.py
```

### 🤖 Fitur Auto-Redirection & Fallback
Script `01_bronze.py` telah dilengkapi fitur cerdas untuk menangani berbagai kendala Environment lokal:
* **Pengguna Windows Tanpa HADOOP_HOME:** Jika HADOOP_HOME tidak ditemukan, script akan *otomatis mendeteksi* dan mengalihkan (*redirect*) eksekusi PySpark ke dalam container Docker `spark-master`. Anda cukup menjalankan perintah `python lakehouse/01_bronze.py` seperti biasa, sistem akan menanganinya di latar belakang!
* **Fallback File Lokal:** Jika koneksi HDFS/Hadoop NameNode benar-benar gagal (baik di Windows maupun Docker), script otomatis beralih membaca dari file live lokal (`dashboard/data/live_api.json` dan `live_rss.json`).
  * *Keterbatasan fallback lokal:* Data yang diambil hanya snapshot saat itu, tidak mencakup file log historis dari Kafka di HDFS.

---

## 📊 Output yang Dihasilkan

Setelah script berhasil dijalankan, data akan tersimpan dalam format **Delta Lake (Parquet + Transaction Log)** pada path relatif berikut:

```text
kelompok-1-ets-bigdata/
└── lakehouse/
    └── lakehouse_data/
        └── bronze/
            ├── gempa_api/   <-- Menyimpan tabel Delta untuk USGS API
            │   ├── _delta_log/
            │   └── part-*.parquet
            └── gempa_rss/   <-- Menyimpan tabel Delta untuk Google News RSS
                ├── _delta_log/
                └── part-*.parquet
```

---

## 🔍 Verifikasi Hasil Ingest

Untuk memastikan tabel Delta telah terbentuk dengan benar di mesin lokal Anda, Anda bisa mengecek isi direktori output menggunakan command berikut di terminal:

```bash
# Cek isi folder output Delta
ls -la lakehouse/lakehouse_data/bronze/gempa_api/
ls -la lakehouse/lakehouse_data/bronze/gempa_rss/
```
Pastikan folder `_delta_log/` dan file data `.parquet` sudah terbuat di masing-masing direktori.
