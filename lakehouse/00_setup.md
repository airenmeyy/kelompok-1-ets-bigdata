# Setup Spark + Delta Lake untuk GempaRadar Lakehouse

Dokumen ini berisi panduan untuk melakukan setup environment, menjalankan pipeline data **Bronze Layer** (Ingestion) dan **Silver Layer** (Pembersihan & Time Travel) pada sistem GempaRadar. Panduan ini ditujukan bagi seluruh anggota Kelompok 1 ETS Big Data.

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

## 🥉 Cara Menjalankan Bronze Layer (Anggota 1)

Pipeline Bronze Layer dapat dieksekusi langsung menggunakan python script. Pastikan terminal berada di root project (`kelompok-1-ets-bigdata/`):

```bash
# Jalankan script Bronze Layer
python lakehouse/01_bronze.py
```

### 🤖 Fitur Auto-Redirection & Fallback
Script `01_bronze.py` telah dilengkapi fitur cerdas untuk menangani berbagai kendala environment lokal:
* **Pengguna Windows Tanpa HADOOP_HOME:** Jika HADOOP_HOME tidak ditemukan, script akan *otomatis mendeteksi* dan mengalihkan (*redirect*) eksekusi PySpark ke dalam container Docker `spark-master`. Anda cukup menjalankan perintah `python lakehouse/01_bronze.py` seperti biasa, sistem akan menanganinya di latar belakang!
* **Fallback File Lokal:** Jika koneksi HDFS/Hadoop NameNode benar-benar gagal, script otomatis beralih membaca dari file live lokal (`dashboard/data/live_api.json` dan `live_rss.json`) dengan skema `file://` agar dibaca dengan sukses oleh Spark dalam container.

---

## 🥈 Cara Menjalankan Silver Layer & Time Travel (Anggota 2)

Setelah data Bronze berhasil terbentuk, Anda dapat mengeksekusi pipeline **Silver Layer** untuk melakukan pembersihan data (*data cleaning*) dan menjalankan demonstrasi **Time Travel** Delta Lake:

```bash
# Jalankan script Silver Layer
python lakehouse/02_silver.py
```

### ⚙️ Transformasi & Pembersihan yang Dilakukan
* **Silver API (6 Tahap):**
  1. `dropDuplicates(["id"])` — Mencegah bias/double-count gempa berdasarkan ID unik USGS.
  2. `filter(magnitude >= 0)` — Membuang magnitudo tidak valid (negatif).
  3. `filter(depth_km > 0)` — Membuang kedalaman gempa tidak logis ($\le 0$ km).
  4. `filter(place.isNotNull())` — Membuang data tanpa lokasi.
  5. `to_timestamp("event_time")` — Mengubah string ISO ke tipe `TimestampType` agar dapat dioperasikan secara temporal.
  6. `hour(event_time)` $\rightarrow$ kolom `jam_kejadian` & `tanggal_kejadian` — Mengekstrak jam dan tanggal kejadian untuk pola analisis temporal.
* **Silver RSS (5 Tahap):**
  1. `dropDuplicates(["id"])` — Menghindari duplikasi berita dari ingestion berulang.
  2. `filter(title.isNotNull())` — Menyaring artikel kosong tanpa judul.
  3. `to_timestamp("published_time")` — Menyamakan tipe data ke tipe `TimestampType`.
  4. `regexp_replace(summary, HTML_PATTERN, "")` $\rightarrow$ kolom `summary_clean` — Membersihkan tag-tag HTML agar teks bersih untuk NLP/analisis sentimen.
  5. `to_date(published_time)` $\rightarrow$ kolom `tanggal_terbit` — Agregasi tren harian artikel.

### ⏱️ Demonstrasi Time Travel (Wajib)
Script ini mendemonstrasikan kekuatan pencatatan riwayat transaksi (*transaction log*) Delta Lake:
1. **Pembaruan (UPDATE):** Mengubah `mag_category` menjadi `"sangat_kuat"` untuk gempa $\ge 5.0$ SR (menghasilkan **Version 1**).
2. **Perbandingan (Comparison):** Menampilkan distribusi magnitudo di **Version 0** vs **Version 1**.
3. **Pencarian Waktu (Time Query):** Menggunakan `.option("timestampAsOf", timestamp)` untuk membaca keadaan data pada waktu tertentu di masa lalu.
4. **Pemulihan (RESTORE):** Mengembalikan tabel kembali ke kondisi awal dengan `restoreToVersion(0)` (menghasilkan versi terbaru di log riwayat).

---

## 🥇 Cara Menjalankan Gold Layer — Reproduksi ETS & Enhanced Data(Anggota 3 & 4)

Setelah data Silver API dan RSS sukses dibentuk, jalankan pipeline Gold Layer gabungan. File 03_gold.py memproses Reproduksi Analisis ETS (Tugas Anggota 3) sekaligus Analisis Pengayaan/Enhancement (Tugas Anggota 4) dalam satu sesi Spark terpadu.

```bash
# Jalankan script Gold Layer
python lakehouse/03_gold.py
```

### ⚙️ Matriks Output Analisis (Combined Gold Layer)

Script ini secara otomatis membagi beban komputasi menjadi dua kelompok metrik utama dan mengeluarkan 7 output tabel Delta:

| No | Analisis | Metode | Output Gold Delta | Kontributor |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Distribusi Magnitudo** | DataFrame API (`when/otherwise`) | `gold/ets_distribusi_magnitudo` | Anggota 3 |
| **2** | **Top 10 Wilayah Aktif** | Spark SQL (`REGEXP_REPLACE + GROUP BY`) | `gold/ets_top_wilayah` | Anggota 3 |
| **3** | **Distribusi & Statistik Kedalaman** | Spark SQL (`SUM CASE WHEN + AVG/MAX/MIN`) | `gold/ets_distribusi_kedalaman` | Anggota 3 |
| **4** | **Statistik Ringkasan** | Spark SQL (`COUNT, AVG, MAX, MIN, STDDEV`) | `gold/ets_statistik_ringkasan` | Anggota 3 |
| **5** | **Skor Risiko Wilayah** | Formula Kuantitatif (Freq × Avg Mag) | `gold/gempa_risk_score` | Anggota 4 |
| **6** | **Korelasi Berita Gempa** | Cross-Source Join (API + RSS)🌟 | `gold/gempa_significant_alerts`| Anggota 4 |
| **Bonus** | **Spark MLlib Linear Regression** | `VectorAssembler + LinearRegression` | `gold/ets_mllib_tren` | Anggota 3 |

### 🌟 Fitur Unggulan: Cross-Source Join 

Pada tabel gempa_significant_alerts, Spark melakukan conditional inner join lintas platform data yang tidak memiliki ID penghubung bawaan. Data seismik fisik dari Silver API dan data narasi berita dari Silver RSS digabungkan menggunakan korelasi kontekstual berbasis jendela waktu (temporal window constraint):

> Kondisi Join=Waktu Terbit Berita∈[Waktu Kejadian Gempa,Waktu Kejadian Gempa+2 Jam]

### 📝 Catatan Penting
- Seluruh input data Silver API dan RSS dibaca bersamaan di bagian awal script menggunakan prinsip Fail-Fast untuk meminimalisir kegagalan parsial di tengah cluster.
- Script menghasilkan file ringkasan terintegrasi dashboard/data/spark_results.json untuk visualisasi Flask Dashboard.
- Gunakan perintah F5 (Reload) atau nyalakan ulang server web Flask Anda setelah skrip memunculkan log SUCCESS agar visualisasi menarik data terbaru dari file log Delta.

---

## 📊 Output yang Dihasilkan

Setelah ketiga script berhasil dijalankan, data akan tersimpan dalam format **Delta Lake (Parquet + Transaction Log)** pada path relatif berikut:

```text
kelompok-1-ets-bigdata/
└── lakehouse/
    └── lakehouse_data/
        ├── bronze/
        │   ├── gempa_api/                  <-- Delta Layer Ingestion API
        │   │   ├── _delta_log/
        │   │   └── part-*.parquet
        │   └── gempa_rss/                  <-- Delta Layer Ingestion RSS
        │       ├── _delta_log/
        │       └── part-*.parquet
        ├── silver/
        │   ├── gempa_api/                  <-- Data API Bersih (Deduped, Valid)
        │   │   ├── _delta_log/
        │   │   └── part-*.parquet
        │   └── gempa_rss/                  <-- Data RSS Bersih (No HTML)
        │       ├── _delta_log/
        │       └── part-*.parquet
        └── gold/
            ├── ets_distribusi_magnitudo/   <-- Kategori Mikro ~ Kuat (ETS)
            │   ├── _delta_log/
            │   └── part-*.parquet
            ├── ets_top_wilayah/            <-- Ranking 10 Wilayah Teraktif (ETS)
            │   ├── _delta_log/
            │   └── part-*.parquet
            ├── ets_distribusi_kedalaman/   <-- Statistik Kedalaman Gempa (ETS)
            │   ├── _delta_log/
            │   └── part-*.parquet
            ├── ets_statistik_ringkasan/    <-- Gabungan Summary Stats (ETS)
            │   ├── _delta_log/
            │   └── part-*.parquet
            ├── ets_mllib_tren/             <-- Model Prediksi Tren Linear Regression (ETS)
            │   ├── _delta_log/
            │   └── part-*.parquet
            ├── gempa_risk_score/           <-- METRIK BARU: Indeks Kerawanan Wilayah
            │   ├── _delta_log/
            │   └── part-*.parquet
            └── gempa_significant_alerts/   <-- METRIK BARU: Hasil Cross-Source Join API+RSS
                ├── _delta_log/
                └── part-*.parquet
```

---

## 🔍 Verifikasi Hasil Pemrosesan

Untuk memastikan tabel Delta telah terbentuk dengan benar di mesin lokal Anda, Anda bisa mengecek isi direktori output menggunakan command berikut di terminal:

```bash
# 1. Cek isi folder output Bronze
ls lakehouse/lakehouse_data/bronze/gempa_api/
ls lakehouse/lakehouse_data/bronze/gempa_rss/

# 2. Cek isi folder output Silver
ls lakehouse/lakehouse_data/silver/gempa_api/
ls lakehouse/lakehouse_data/silver/gempa_rss/

# 3. Cek isi folder output Gold 
ls lakehouse/lakehouse_data/gold/ets_distribusi_magnitudo/
ls lakehouse/lakehouse_data/gold/ets_top_wilayah/
ls lakehouse/lakehouse_data/gold/ets_distribusi_kedalaman/
ls lakehouse/lakehouse_data/gold/ets_statistik_ringkasan/
ls lakehouse/lakehouse_data/gold/ets_mllib_tren/
ls lakehouse/lakehouse_data/gold/gempa_risk_score/
ls lakehouse/lakehouse_data/gold/gempa_significant_alerts/
```
Pastikan folder `_delta_log/` (log transaksi) dan file data `.parquet` sudah terbuat di masing-masing direktori.
