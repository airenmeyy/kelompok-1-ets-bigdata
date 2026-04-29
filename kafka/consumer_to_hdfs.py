import json
import time
import threading
import os
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

# --- OPSI B: Setup HDFS Client ---
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

# Siapkan folder lokal untuk Dashboard (sebagai backup live)
DASHBOARD_DIR = '../dashboard/data'
os.makedirs(DASHBOARD_DIR, exist_ok=True)

# Buffer penampung sementara & Lock untuk mencegah bentrok antar Thread
buffer_api = []
buffer_rss = []
lock = threading.Lock()

# --- HINT: Gunakan threading untuk membaca 2 topic secara paralel ---
def consume_api():
    """Thread 1: Khusus membaca topic gempa-api"""
    consumer_api = KafkaConsumer(
        'gempa-api',
        bootstrap_servers=['localhost:9092'],
        group_id='hdfs-consumer-api-group', # HINT: group_id unik
        auto_offset_reset='earliest',       # HINT: auto_offset_reset="earliest"
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer_api:
        with lock:
            buffer_api.append(message.value)

def consume_rss():
    """Thread 2: Khusus membaca topic gempa-rss"""
    consumer_rss = KafkaConsumer(
        'gempa-rss',
        bootstrap_servers=['localhost:9092'],
        group_id='hdfs-consumer-rss-group', # HINT: group_id unik
        auto_offset_reset='earliest',       # HINT: auto_offset_reset="earliest"
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer_rss:
        with lock:
            buffer_rss.append(message.value)

def flush_to_hdfs():
    """Thread 3: Setiap 2 menit menyimpan buffer ke HDFS"""
    while True:
        time.sleep(120) # HINT: setiap 2–5 menit simpan buffer
        
        # HINT: Nama file di HDFS gunakan timestamp
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
        
        # Amankan data dari buffer dan kosongkan untuk periode berikutnya
        with lock:
            data_api_to_save = buffer_api.copy()
            data_rss_to_save = buffer_rss.copy()
            buffer_api.clear()
            buffer_rss.clear()

        # Simpan Data API
        if data_api_to_save:
            api_path = f"/data/gempa/api/{timestamp_str}.json"
            try:
                # Menulis langsung ke HDFS
                hdfs_client.write(api_path, data=json.dumps(data_api_to_save), encoding='utf-8')
                
                # Simpan juga ke lokal untuk dashboard (sesuai arsitektur)
                with open(os.path.join(DASHBOARD_DIR, 'live_api.json'), 'w') as f:
                    json.dump(data_api_to_save, f)
                print(f"[{timestamp_str}] Sukses HDFS: Tersimpan {len(data_api_to_save)} data di {api_path}")
            except Exception as e:
                print(f"Error HDFS API: {e}")

        # Simpan Data RSS
        if data_rss_to_save:
            rss_path = f"/data/gempa/rss/{timestamp_str}.json"
            try:
                # Menulis langsung ke HDFS
                hdfs_client.write(rss_path, data=json.dumps(data_rss_to_save), encoding='utf-8')
                
                # Simpan juga ke lokal untuk dashboard
                with open(os.path.join(DASHBOARD_DIR, 'live_rss.json'), 'w') as f:
                    json.dump(data_rss_to_save, f)
                print(f"[{timestamp_str}] Sukses HDFS: Tersimpan {len(data_rss_to_save)} berita di {rss_path}")
            except Exception as e:
                print(f"Error HDFS RSS: {e}")


if __name__ == "__main__":
    print("Memulai HDFS Consumer dengan Threading (Menunggu 2 Menit untuk penyimpanan pertama)...")
    
    # Jalankan ketiga fungsi secara paralel menggunakan Thread
    thread_api = threading.Thread(target=consume_api, daemon=True)
    thread_rss = threading.Thread(target=consume_rss, daemon=True)
    thread_flush = threading.Thread(target=flush_to_hdfs, daemon=True)

    thread_api.start()
    thread_rss.start()
    thread_flush.start()

    # Agar program utama tidak langsung tertutup
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nConsumer HDFS dihentikan.")