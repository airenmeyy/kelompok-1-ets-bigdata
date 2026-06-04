while ($true) {
    Write-Host "=========================================="
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Memulai Sinkronisasi Lakehouse (Bronze -> Silver -> Gold)"
    Write-Host "=========================================="
    
    Write-Host ">>> Menjalankan 01_bronze.py..."
    ..\.venv\Scripts\python .\lakehouse\01_bronze.py
    
    Write-Host ">>> Menjalankan 02_silver.py..."
    ..\.venv\Scripts\python .\lakehouse\02_silver.py
    
    Write-Host ">>> Menjalankan 03_gold.py..."
    ..\.venv\Scripts\python .\lakehouse\03_gold.py
    
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Sinkronisasi Selesai."
    Write-Host "Menunggu 60 detik sebelum update berikutnya..."
    Start-Sleep -Seconds 60
}
