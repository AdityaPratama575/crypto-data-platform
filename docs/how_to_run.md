# How To Run - Local v1 Ingestion

Dokumen ini menjelaskan cara menjalankan pipeline v1 lokal untuk alur raw awal:

- `/coins/list` -> `crypto_raw.raw_coin_list_snapshot`
- `/coins/markets` top 100 -> `crypto_raw.raw_coin_markets_snapshot`
- `/global` -> `crypto_raw.raw_global_snapshot`
- `/coins/{id}/market_chart` -> `crypto_raw.raw_coin_market_chart_point`

Lalu saat Airflow aktif, alur di atas akan lanjut merge/upsert ke `crypto_core`:
- `dim_coin`
- `fact_coin_market_snapshot`
- `fact_global_market`
- `fact_coin_price_history`

Alur manual tetap bisa dijalankan **tanpa Airflow dulu** untuk validasi end-to-end yang cepat.

Dokumen ini juga mencakup transisi baseline ke Airflow Docker untuk DAG pertama.
Status flow terkini bisa dilihat di `docs/pipeline_flow.md`.
Runbook operasional ada di `docs/runbook.md`.

---

## 1. Prasyarat

- Linux/WSL environment
- Python 3.10+ (disarankan pakai virtualenv)
- Akses ke MySQL lokal yang sudah reachable dari environment Python
- CoinGecko Demo API key di `.env`

---

## 2. Setup

### 2.1 Buat virtualenv dan install dependency

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2.2 Siapkan `.env`

```bash
cp .env.example .env
```

Isi variabel penting:

- `COINGECKO_API_KEY`
- `COINGECKO_BASE_URL`
- `COINGECKO_VS_CURRENCY`
- `TRACKED_COINS_TOP_N` (v1: `100`)
- `RAW_DATA_DIR` (default: `data/raw`)
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`
- `MYSQL_DATABASE_RAW`, `MYSQL_DATABASE_CORE`, `MYSQL_DATABASE_MART`, `MYSQL_DATABASE_OPS`

Opsional tuning khusus `coin_history_daily_dag`:

- `COIN_HISTORY_BATCH_SIZE` (default: `10`)
- `COIN_HISTORY_REQUEST_PAUSE_SECONDS` (default: `1.5`)
- `COIN_HISTORY_BATCH_PAUSE_SECONDS` (default: `8.0`)
- `COIN_HISTORY_MAX_RETRIES` (default: `6`)
- `COIN_HISTORY_BACKOFF_SECONDS` (default: `4`)

Opsional retention cleanup mart:

- `MART_RETENTION_ENABLED` (default runtime: `false`)
- `MART_TOP_COINS_RETENTION_DAYS` (default: `90`)
- `MART_MARKET_SUMMARY_RETENTION_DAYS` (default: `0`)
- `MART_COIN_DAILY_RETENTION_DAYS` (default: `730`)
- `MART_COIN_ROLLING_RETENTION_DAYS` (default: `730`)

Opsional ops alerting:

- `OPS_ALERT_LOOKBACK_HOURS` (default: `6`)
- `OPS_ALERT_MAX_ITEMS` (default: `20`)
- `OPS_ALERT_EXCLUDE_DAG_IDS` (default: `ops_alerting_dag`)

### 2.3 Siapkan `.env.docker` (override untuk container)

File ini dipakai oleh `docker-compose.yml` hanya untuk runtime container.

Contoh isi saat ini:

```env
MYSQL_HOST=host.docker.internal
AIRFLOW_UID=1000
```

Tujuan:
- script lokal WSL tetap bisa pakai host/IP di `.env`
- container Airflow bisa connect ke MySQL host Windows dengan hostname Docker
- container Airflow bisa menulis ke volume mount (`./logs`) tanpa error permission

### 2.4 Inisialisasi schema

```bash
.venv/bin/python scripts/init_db.py
```

---

## 3. Smoke Test Dasar

Pastikan koneksi ke MySQL dan CoinGecko berfungsi:

```bash
.venv/bin/python scripts/smoke_test.py
```

Jika berhasil, output akan berisi status `[OK]` untuk MySQL dan CoinGecko.

---

## 4. Jalankan Ingestion Lokal (Tanpa Airflow)

### 4.1 Coin list

```bash
.venv/bin/python scripts/run_local_extract.py --flow coin_list
```

Expected summary:

- `batch_id=coin_list_...`
- `raw_file=data/raw/coins_list/...json`
- `payload_count` sekitar belasan ribu+
- `inserted_rows` sama dengan `payload_count`

### 4.2 Coin markets top 100

```bash
.venv/bin/python scripts/run_local_extract.py --flow coin_markets
```

Expected summary:

- `batch_id=coin_markets_...`
- `raw_file=data/raw/coins_markets/...json`
- `payload_count=100`
- `inserted_rows=100`

### 4.3 Jalankan keduanya

```bash
.venv/bin/python scripts/run_local_extract.py --flow all
```

### 4.4 Global market metrics

```bash
.venv/bin/python scripts/run_local_extract.py --flow global
```

Expected summary:

- `batch_id=global_...`
- `raw_file=data/raw/global/...json`
- `payload_count=1`
- `inserted_rows=1`

### 4.5 Coin history market chart (raw points)

Contoh validasi cepat (5 coins, 1 hari):

```bash
.venv/bin/python scripts/run_local_extract.py --flow coin_history --history-top-n 5 --history-days 1
```

Contoh target tracked policy penuh:

```bash
.venv/bin/python scripts/run_local_extract.py --flow coin_history --history-top-n 100 --history-days 1
```

Expected summary:

- `batch_id=coin_history_...`
- `raw_file=data/raw/coin_market_chart/* (... files)`
- `payload_count` = jumlah coin yang diproses
- `inserted_rows` = total point history yang berhasil dimuat

Catatan:
- flow history sekarang memakai throttling berbasis batch + retry khusus history untuk menurunkan risiko `429`.
- tuning bisa via `.env` (variabel `COIN_HISTORY_*`) atau `dag_run.conf`.

---

## 5. Validasi Data Masuk ke MySQL

### 5.1 Cek batch terbaru coin list + coin markets + global + history

```bash
.venv/bin/python - <<'PY'
from src.db import mysql_connection

with mysql_connection(layer='raw') as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT batch_id, COUNT(*) AS row_count, MAX(extracted_at_utc) AS extracted_at_utc
            FROM raw_coin_list_snapshot
            GROUP BY batch_id
            ORDER BY extracted_at_utc DESC
            LIMIT 3
        """)
        print("coin_list:", cur.fetchall())

        cur.execute("""
            SELECT batch_id, COUNT(*) AS row_count, MAX(extracted_at_utc) AS extracted_at_utc
            FROM raw_coin_markets_snapshot
            GROUP BY batch_id
            ORDER BY extracted_at_utc DESC
            LIMIT 3
        """)
        print("coin_markets:", cur.fetchall())

        cur.execute("""
            SELECT batch_id, COUNT(*) AS row_count, MAX(extracted_at_utc) AS extracted_at_utc
            FROM raw_global_snapshot
            GROUP BY batch_id
            ORDER BY extracted_at_utc DESC
            LIMIT 3
        """)
        print("global:", cur.fetchall())

        cur.execute("""
            SELECT batch_id, COUNT(*) AS row_count, MAX(extracted_at_utc) AS extracted_at_utc
            FROM raw_coin_market_chart_point
            GROUP BY batch_id
            ORDER BY extracted_at_utc DESC
            LIMIT 3
        """)
        print("coin_history:", cur.fetchall())
PY
```

### 5.2 Cek file raw JSON

```bash
find data/raw -maxdepth 2 -type f | sort | tail -n 10
```

---

## 6. Komponen Kode yang Terlibat

- `src/clients/coingecko_client.py`: HTTP client CoinGecko (auth, timeout, retry)
- `src/db/mysql.py`: helper koneksi MySQL berbasis `.env`
- `src/extract/fetch_coin_list.py`: extract `/coins/list`
- `src/extract/fetch_coin_markets.py`: extract `/coins/markets` top N
- `src/extract/fetch_global_metrics.py`: extract `/global`
- `src/extract/fetch_coin_history.py`: extract `/coins/{id}/market_chart` untuk tracked coins
- `src/load/write_raw_json.py`: simpan raw envelope JSON
- `src/load/load_raw_tables.py`: append ke tabel raw MySQL
- `src/observability/ops_writer.py`: tulis `pipeline_run`, `task_run`, dan `dq_check_result` ke `crypto_ops`
- `src/quality/checks.py`: DQ checks lintas layer (`raw`, `core`, `mart`) per DAG, termasuk structural + business rules
- `src/transform/build_marts.py`: refresh/upsert mart dari `crypto_core` ke `crypto_mart`
- `scripts/run_local_extract.py`: runner manual lokal tanpa Airflow

---

## 7. Troubleshooting Cepat

### Error: `ModuleNotFoundError: No module named 'dotenv'`
- Penyebab: command tidak jalan dengan environment `.venv`.
- Solusi: pakai `.venv/bin/python ...` atau aktifkan virtualenv dulu.

### Error koneksi CoinGecko (`NameResolutionError`, timeout)
- Penyebab: DNS/network environment tidak bisa keluar.
- Solusi: cek internet, proxy, firewall, atau policy sandbox/network runtime.

### Error koneksi MySQL (`Can't connect`, `Operation not permitted`)
- Penyebab: host/port tidak reachable dari runtime atau policy sandbox.
- Solusi: cek `MYSQL_HOST`/`MYSQL_PORT`, pastikan MySQL aktif, dan pastikan environment runtime punya akses jaringan ke host MySQL.

### Error schema/table tidak ditemukan
- Penyebab: DDL belum dijalankan.
- Solusi: jalankan ulang:
  - `.venv/bin/python scripts/init_db.py`

---

## 8. Catatan Keamanan

- Jangan commit `.env`
- Jangan hardcode API key/password di source code
- Jangan print secret ke output/log
- Gunakan `.env.example` hanya untuk placeholder

---

## 9. Jalankan Airflow Docker (Baseline DAG 1)

### 9.1 Inisialisasi Airflow metadata DB + admin user

```bash
make airflow-init
```

### 9.2 Jalankan stack Airflow

```bash
make up
make ps
```

### 9.3 Akses UI dan trigger DAG

- URL: `http://localhost:8080`
- username: `admin`
- password: `admin`
- DAG baseline saat ini:
  - `reference_coin_list_dag`
  - `market_snapshot_hourly_dag`
  - `global_market_hourly_dag`
  - `coin_history_daily_dag`
  - `ops_alerting_dag`

Langkah:
1. Unpause DAG `reference_coin_list_dag`
2. Trigger manual satu run dari UI
3. Cek task `extract_write_load_coin_list` sukses
4. Unpause DAG `market_snapshot_hourly_dag`
5. Trigger manual satu run dari UI
6. Cek task `extract_write_load_coin_markets` sukses
7. Unpause DAG `global_market_hourly_dag`
8. Trigger manual satu run dari UI
9. Cek task `extract_write_load_global` sukses
10. Unpause DAG `coin_history_daily_dag`
11. Trigger manual satu run dari UI (opsional conf validasi cepat: `{"top_n": 5, "days": 1, "batch_size": 5, "request_pause_seconds": 0.2, "batch_pause_seconds": 0}`)
12. Cek task `extract_write_load_coin_history` sukses
13. Unpause DAG `ops_alerting_dag`
14. Trigger manual satu run dari UI
15. Cek task `check_ops_alerts`

Catatan:
- Jika `check_ops_alerts` berstatus `failed`, itu berarti alert memang terdeteksi di `crypto_ops` (behavior by design).

Catatan penting:
- CoinGecko Demo API bisa mengembalikan `429` pada flow `coin_history` jika langsung menjalankan top 100 penuh dalam satu task.
- Untuk validasi baseline awal, gunakan conf kecil dulu (mis. `top_n=5`) lalu naikkan bertahap sesuai budget/rate limit.
- Jika run top 100 belum stabil, tingkatkan `client_max_retries` / `client_backoff_seconds` dan/atau tambah pause (`request_pause_seconds`, `batch_pause_seconds`).
- Untuk baseline stabilitas top 100, gunakan dulu:
  - `batch_size=10`
  - `request_pause_seconds=1.5`
  - `batch_pause_seconds=8.0`
  - `client_max_retries=6`
  - `client_backoff_seconds=4`
- Issue duplicate timestamp raw pada `coin_history` (yang terlihat pada run lama) sudah diperbaiki dengan preservasi presisi millisecond saat parsing epoch.

### 9.4 Monitoring stabilitas history & ops alert

```bash
.venv/bin/python scripts/monitor_coin_history_stability.py --lookback-days 7 --max-rows 200
.venv/bin/python scripts/check_ops_alerts.py --lookback-hours 24 --exclude-dag-id ops_alerting_dag
```

### 9.5 Validasi hasil run DAG

```bash
.venv/bin/python - <<'PY'
from src.db import mysql_connection

with mysql_connection(layer='raw') as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT batch_id, COUNT(*) AS row_count, MAX(extracted_at_utc) AS extracted_at_utc
            FROM raw_coin_list_snapshot
            GROUP BY batch_id
            ORDER BY extracted_at_utc DESC
            LIMIT 5
        """)
        print(cur.fetchall())

        cur.execute("""
            SELECT batch_id, COUNT(*) AS row_count, MAX(extracted_at_utc) AS extracted_at_utc
            FROM raw_coin_markets_snapshot
            GROUP BY batch_id
            ORDER BY extracted_at_utc DESC
            LIMIT 5
        """)
        print(cur.fetchall())

        cur.execute("""
            SELECT batch_id, COUNT(*) AS row_count, MAX(extracted_at_utc) AS extracted_at_utc
            FROM raw_global_snapshot
            GROUP BY batch_id
            ORDER BY extracted_at_utc DESC
            LIMIT 5
        """)
        print(cur.fetchall())

        cur.execute("""
            SELECT batch_id, COUNT(*) AS row_count, COUNT(DISTINCT coin_id) AS coin_count,
                   MAX(extracted_at_utc) AS extracted_at_utc
            FROM raw_coin_market_chart_point
            GROUP BY batch_id
            ORDER BY extracted_at_utc DESC
            LIMIT 5
        """)
        print(cur.fetchall())

with mysql_connection(layer='ops') as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT dag_id, dag_run_id, run_status, started_at_utc, ended_at_utc
            FROM pipeline_run
            ORDER BY pipeline_run_id DESC
            LIMIT 10
        """)
        print(cur.fetchall())

        cur.execute("""
            SELECT dag_id, dag_run_id, task_id, task_status, rows_extracted, rows_loaded
            FROM task_run
            ORDER BY task_run_id DESC
            LIMIT 10
        """)
        print(cur.fetchall())

        cur.execute("""
            SELECT dag_id, dag_run_id, check_name, check_status, failed_row_count
            FROM dq_check_result
            ORDER BY dq_result_id DESC
            LIMIT 20
        """)
        print(cur.fetchall())

        cur.execute("""
            SELECT dag_run_id, target_table, check_name, check_status, failed_row_count
            FROM dq_check_result
            WHERE check_name IN (
                'first_seen_lte_last_seen',
                'market_cap_rank_positive',
                'market_cap_pct_btc_in_range',
                'close_price_within_min_max',
                'rolling_metrics_non_negative'
            )
            ORDER BY dq_result_id DESC
            LIMIT 20
        """)
        print("business_dq_samples:", cur.fetchall())

with mysql_connection(layer='core') as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source_batch_id, COUNT(*) AS row_count
            FROM fact_coin_market_snapshot
            GROUP BY source_batch_id
            ORDER BY MAX(updated_at_utc) DESC
            LIMIT 5
        """)
        print("core_fact_coin_market_snapshot:", cur.fetchall())

        cur.execute("""
            SELECT source_batch_id, COUNT(*) AS row_count
            FROM fact_global_market
            GROUP BY source_batch_id
            ORDER BY MAX(updated_at_utc) DESC
            LIMIT 5
        """)
        print("core_fact_global_market:", cur.fetchall())

        cur.execute("""
            SELECT source_batch_id, COUNT(*) AS row_count
            FROM fact_coin_price_history
            GROUP BY source_batch_id
            ORDER BY MAX(updated_at_utc) DESC
            LIMIT 5
        """)
        print("core_fact_coin_price_history:", cur.fetchall())
PY
```

Catatan:
- Untuk tabel core dengan grain unik, run lebih baru pada grain yang sama dapat meng-update baris lama.
- Karena itu, `source_batch_id` pada tabel fact bisa berubah mengikuti run terbaru.

### 9.6 Stop Airflow

```bash
make down
```

### 9.7 Alternatif command dari Windows `.bat`

Jika menjalankan dari Windows host (bukan shell WSL), gunakan wrapper berikut:

```bat
scripts\windows\airflow_init.bat
scripts\windows\airflow_up.bat
scripts\windows\reference_coin_list_trigger.bat
scripts\windows\market_snapshot_trigger.bat
scripts\windows\global_market_trigger.bat
scripts\windows\coin_history_trigger.bat
scripts\windows\ops_alerting_trigger.bat
scripts\windows\airflow_down.bat
```

### 9.8 Jika Docker gagal pull image

Contoh error:
- `lookup registry-1.docker.io: no such host`
- `Docker Desktop has no HTTPS proxy`

Artinya runtime Docker belum bisa reach Docker Hub.

Checklist cepat:
- pastikan Docker Desktop online
- cek DNS internet di host Windows
- jika pakai proxy kantor, set proxy di Docker Desktop
- retry `make airflow-init` setelah konektivitas normal

### 9.9 Jika muncul `Permission denied: /opt/airflow/logs/scheduler`

Penyebab umum:
- UID user di container tidak cocok dengan ownership volume host.

Solusi:
- pastikan ada `AIRFLOW_UID=1000` di `.env.docker` (atau sesuaikan dengan `id -u` host kamu).
