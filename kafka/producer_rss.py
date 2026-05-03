import json
import time
import logging
import hashlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
 
import feedparser
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
 
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_NAME = "gempa-rss"
POLLING_INTERVAL_SECONDS = 30      # 30 detik (untuk demo)
 
RSS_FEEDS = [
    {
        "name": "Google News (Gempa Indonesia)",
        "url": "https://news.google.com/rss/search?q=gempa+indonesia&hl=id&gl=ID&ceid=ID:id",
        "priority": 1,
    }
]
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("producer_rss")
 

def make_key(url: str) -> str:
    """
    [NamaAnggota3]: Key = hash 8 karakter dari URL artikel.
    Sesuai instruksi ETS: 'key: hash 8 karakter dari URL artikel'
    """
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:8]
 
 
# ── Helper: parse waktu publikasi RSS ────────────────────────────────────
def parse_published_time(entry) -> str:
   
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass
 
    # Cara 2: pakai string published mentah
    if hasattr(entry, "published") and entry.published:
        try:
            dt = parsedate_to_datetime(entry.published)
            return dt.isoformat()
        except Exception:
            pass
 
    # Fallback: waktu sekarang
    return datetime.now(timezone.utc).isoformat()
 
 
# ── Helper: bersihkan teks HTML dari summary ─────────────────────────────
def clean_summary(text: str, max_len: int = 500) -> str:
    """[NamaAnggota3]: Hapus tag HTML dasar dan potong teks."""
    import re
    # Hapus tag HTML
    text = re.sub(r"<[^>]+>", " ", text or "")
    # Hapus whitespace berlebih
    text = " ".join(text.split())
    return text[:max_len]
 
 
# ── Helper: fetch & parse single RSS feed ────────────────────────────────
def fetch_rss_entries(feed_config: dict) -> list[dict]:
    """
    [NamaAnggota3]: Parse RSS feed menggunakan feedparser.
    Field yang diambil: title, link, summary, published.
    """
    url = feed_config["url"]
    source_name = feed_config["name"]
 
    try:
        # feedparser.parse() langsung bisa handle URL
        feed = feedparser.parse(url)
 
        if feed.bozo and feed.bozo_exception:
            log.warning("RSS %s: parsing warning — %s", source_name, feed.bozo_exception)
 
        entries = feed.entries
        log.info("RSS %s: %d artikel ditemukan", source_name, len(entries))
        return entries
 
    except Exception as e:
        log.error("Gagal fetch RSS %s (%s): %s", source_name, url, e)
        return []
 
 
# ── Helper: inisialisasi Kafka Producer ──────────────────────────────────
def create_producer() -> KafkaProducer:
 
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        compression_type="gzip",
        request_timeout_ms=30000,
    )
 
 
# ── Tracker deduplikasi ───────────────────────────────────────────────────
class SeenArticleTracker:

    def __init__(self, ttl_seconds: int = 86400):
        self._seen: dict[str, float] = {}
        self._ttl = ttl_seconds
 
    def is_new(self, article_key: str) -> bool:
        now = time.time()
        # Bersihkan entry lama
        expired = [k for k, v in self._seen.items() if now - v > self._ttl]
        for k in expired:
            del self._seen[k]
        if article_key in self._seen:
            return False
        self._seen[article_key] = now
        return True
 
    @property
    def count(self) -> int:
        return len(self._seen)
 
 
# ── Callback Kafka ─────────────────────────────────────────────────────────
def on_send_success(record_metadata):
    log.debug(
        "✓ Terkirim → topic=%s | partition=%d | offset=%d",
        record_metadata.topic, record_metadata.partition, record_metadata.offset,
    )
 
 
def on_send_error(exc):
    log.error("✗ Gagal kirim ke Kafka: %s", exc)
 
 
# ── Main Loop ─────────────────────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("  GempaRadar — Producer RSS")
    log.info("  Topic     : %s", TOPIC_NAME)
    log.info("  Interval  : %d detik (%d menit)", POLLING_INTERVAL_SECONDS, POLLING_INTERVAL_SECONDS // 60)
    log.info("  Feed sources: %s", ", ".join(f["name"] for f in RSS_FEEDS))
    log.info("=" * 55)
 
    producer = create_producer()
    tracker = SeenArticleTracker(ttl_seconds=86400)
    cycle = 0
 
    try:
        while True:
            cycle += 1
            log.info("── Cycle #%d | %s ──", cycle, datetime.now().strftime("%H:%M:%S"))
            total_sent = 0
 
            for feed_config in RSS_FEEDS:
                entries = fetch_rss_entries(feed_config)
                sent_from_feed = 0
                skip_from_feed = 0
 
                for entry in entries:
                    # Ambil URL artikel — ini jadi identifier unik
                    article_url = getattr(entry, "link", "") or getattr(entry, "id", "")
                    if not article_url:
                        continue
 
                    article_key = make_key(article_url)
 
                    # Skip artikel yang sudah pernah dikirim
                    if not tracker.is_new(article_key):
                        skip_from_feed += 1
                        continue
 
                    # ── Struktur event JSON konsisten ──────────────────
                    article_title = getattr(entry, "title", "")
                    article_summary = clean_summary(getattr(entry, "summary", ""))
                    published_time = parse_published_time(entry)
 
                    event = {
                        # Identifikasi
                        "id": article_key,
                        "source": feed_config["name"],
                        "source_priority": feed_config["priority"],
                        # Waktu
                        "timestamp": datetime.now(timezone.utc).isoformat(),  # waktu ingestion
                        "published_time": published_time,                      # waktu publikasi artikel
                        # Konten
                        "title": article_title,
                        "url": article_url,
                        "summary": article_summary,
                        # Tag / kategori (jika ada)
                        "tags": [
                            tag.get("term", "") for tag in getattr(entry, "tags", [])
                        ],
                        # Metadata feed
                        "feed_url": feed_config["url"],
                    }
 
                    producer.send(
                        TOPIC_NAME,
                        key=article_key,
                        value=event,
                    ).add_callback(on_send_success).add_errback(on_send_error)
 
                    log.info(
                        "  → [%s | %s] %s",
                        feed_config["name"],
                        article_key,
                        article_title[:60],
                    )
                    sent_from_feed += 1
                    total_sent += 1
 
                log.info(
                    "  Feed %-7s: %d dikirim, %d di-skip",
                    feed_config["name"], sent_from_feed, skip_from_feed,
                )
 
            producer.flush()
            log.info(
                "  ✓ Cycle #%d selesai: %d total artikel baru | tracked: %d",
                cycle, total_sent, tracker.count,
            )
 
            log.info("  Menunggu %d detik...\n", POLLING_INTERVAL_SECONDS)
            time.sleep(POLLING_INTERVAL_SECONDS)
 
    except KeyboardInterrupt:
        log.info("Producer RSS dihentikan oleh user (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer ditutup dengan bersih.")
 
 
if __name__ == "__main__":
    main()
