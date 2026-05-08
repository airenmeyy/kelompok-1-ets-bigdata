"""
GempaRadar — Spark Auto Runner
Menjalankan spark_processing.py via docker exec setiap INTERVAL_MINUTES menit.
"""
import subprocess
import time
import sys
from datetime import datetime

INTERVAL_MINUTES = 10
CONTAINER = "spark-master"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SCRIPT_PATH = "/app/kafka/spark_processing.py"

CMD = [
    "docker", "exec", CONTAINER,
    SPARK_SUBMIT,
    "--master", "spark://spark-master:7077",
    SCRIPT_PATH,
]

def run_spark():
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{ts}] Menjalankan Spark job...")
    result = subprocess.run(CMD, capture_output=False)
    if result.returncode == 0:
        print(f"[{ts}] Spark job selesai.")
    else:
        print(f"[{ts}] Spark job gagal (exit code {result.returncode}).")
    return result.returncode

if __name__ == "__main__":
    print("=" * 55)
    print("  GempaRadar — Spark Auto Runner")
    print(f"  Interval : {INTERVAL_MINUTES} menit")
    print(f"  Script   : {SCRIPT_PATH}")
    print("=" * 55)

    # Jalankan sekali langsung, lalu loop
    run_spark()

    while True:
        next_run = INTERVAL_MINUTES * 60
        print(f"\nSpark berikutnya dalam {INTERVAL_MINUTES} menit... (Ctrl+C untuk berhenti)")
        time.sleep(next_run)
        run_spark()
