# Crypto Data Platform

Proyek portfolio data engineering berbasis batch yang mengambil data market cryptocurrency dari CoinGecko Demo API ke MySQL lokal menggunakan Python dan Airflow.

Fokus utama repository ini adalah pipeline design, data modeling, observability, dan local development yang rapi dan reproducible.

---

## 1. Tujuan Proyek

Tujuan proyek ini adalah menunjukkan kemampuan data engineering praktis melalui pipeline end-to-end bergaya ETL/ELT batch.

Kemampuan inti yang ingin ditunjukkan:
- extract data dari external API
- orchestration dengan Airflow
- simpan data mentah dan terstruktur ke layered MySQL databases
- preserve raw payload untuk replay/debugging
- implement basic data quality checks
- hasilkan mart tables yang siap dianalisis
- jaga local development tetap aman dan reproducible

Repository ini sengaja **tidak** memasukkan dashboard di v1.

---

## 2. Ringkasan Arsitektur

```text
CoinGecko Demo API
    ->
Python extract/load layer
    ->
Local raw JSON landing zone
    ->
MySQL crypto_raw
    ->
MySQL crypto_core
    ->
MySQL crypto_mart
    ->
MySQL crypto_ops
```

Airflow mengorkestrasi job ingestion dan transformasi utama.

---

## 3. Tech Stack

- Python
- Apache Airflow
- MySQL 8
- Docker / Docker Compose
- SQL
- dotenv / environment variables
- Pytest

Optional local development acceleration:
- VS Code
- MySQL client (CLI/Workbench/DBeaver)

---

## 4. Scope Proyek

### Masuk scope v1
- CoinGecko Demo API ingestion
- raw JSON landing zone
- raw/core/mart/ops database layers
- Airflow DAGs
- SQL transformations
- data quality checks
- operational logging
- local development setup

### Di luar scope v1
- dashboard BI
- dbt
- cloud warehouse
- Kafka / streaming
- production cloud deployment

---

## 5. Domain Data Utama

### Reference data
- daftar/master coin

### Snapshot data
- harga saat ini
- market cap
- market rank
- volume
- perubahan harga

### Historical data
- time series untuk tracked coins

### Global market data
- ringkasan market crypto secara keseluruhan

---

## 6. Struktur Repository

```text
crypto-data-platform/
├── README.md
├── .env.example
├── .env.docker
├── .gitignore
├── requirements.txt
├── docker-compose.yml
├── Makefile
│
├── docs/
│   ├── final_architecture.md
│   ├── how_to_run.md
│   ├── pipeline_flow.md
│   ├── source_contract.md
│   ├── data_model.md
│   └── runbook.md
│
├── config/
│   ├── app_config.yaml
│   ├── pipeline_config.yaml
│   └── tracked_coins.yaml
│
├── dags/
│   ├── reference_coin_list_dag.py
│   ├── market_snapshot_hourly_dag.py
│   ├── global_market_hourly_dag.py
│   ├── coin_history_daily_dag.py
│   └── ops_alerting_dag.py
│
├── src/
│   ├── __init__.py
│   ├── clients/
│   │   ├── __init__.py
│   │   └── coingecko_client.py
│   │
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── fetch_coin_list.py
│   │   ├── fetch_coin_markets.py
│   │   ├── fetch_coin_history.py
│   │   └── fetch_global_metrics.py
│   │
│   ├── load/
│   │   ├── __init__.py
│   │   ├── write_raw_json.py
│   │   ├── load_raw_tables.py
│   │   └── merge_core_tables.py
│   │
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── build_marts.py
│   │   └── mart_retention.py
│   │
│   ├── quality/
│   │   ├── __init__.py
│   │   └── checks.py
│   │
│   ├── observability/
│   │   ├── __init__.py
│   │   ├── alerts.py
│   │   └── ops_writer.py
│   │
│   ├── db/
│   │   ├── __init__.py
│   │   ├── mysql.py
│   │   └── schema_manager.py
│   │
│   └── utils/
│       ├── __init__.py
│       ├── time.py
│       ├── retry.py
│       └── ids.py
│
├── sql/
│   ├── ddl/
│   │   ├── create_crypto_raw.sql
│   │   ├── create_crypto_core.sql
│   │   ├── create_crypto_mart.sql
│   │   └── create_crypto_ops.sql
│   │
│   ├── dml/
│   │   ├── merge_dim_coin.sql
│   │   ├── merge_fact_coin_market_snapshot.sql
│   │   ├── merge_fact_coin_price_history.sql
│   │   ├── merge_fact_global_market.sql
│   │   └── refresh_marts.sql
│   │
│   └── quality/
│       ├── dq_core_checks.sql
│       └── dq_mart_checks.sql
│
├── scripts/
│   ├── check_ops_alerts.py
│   ├── init_db.py
│   ├── monitor_coin_history_stability.py
│   ├── run_backfill.py
│   ├── run_local_extract.py
│   ├── smoke_test.py
│   └── windows/
│       ├── airflow_init.bat
│       ├── airflow_up.bat
│       ├── airflow_down.bat
│       ├── reference_coin_list_trigger.bat
│       ├── market_snapshot_trigger.bat
│       ├── global_market_trigger.bat
│       ├── coin_history_trigger.bat
│       └── ops_alerting_trigger.bat
│
├── tests/
│   ├── test_load_raw_tables.py
│   ├── test_mart_retention.py
│   ├── test_ops_alerts.py
│   ├── test_ops_writer.py
│   └── test_quality_checks.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── sample/
│
└── logs/
```

---

## 7. Environment Variables

Buat file `.env` lokal dari `.env.example`.

Contoh:

```env
COINGECKO_API_KEY=your_demo_api_key_here
COINGECKO_BASE_URL=https://api.coingecko.com/api/v3
COINGECKO_VS_CURRENCY=usd

TRACKED_COINS_MODE=top_n
TRACKED_COINS_TOP_N=100

APP_ENV=local
LOG_LEVEL=INFO
RAW_DATA_DIR=data/raw
REQUEST_TIMEOUT_SECONDS=30
MAX_RETRIES=3
BACKOFF_SECONDS=2

# Optional tuning khusus coin_history_daily_dag
COIN_HISTORY_BATCH_SIZE=10
COIN_HISTORY_REQUEST_PAUSE_SECONDS=1.5
COIN_HISTORY_BATCH_PAUSE_SECONDS=8.0
COIN_HISTORY_MAX_RETRIES=6
COIN_HISTORY_BACKOFF_SECONDS=4

# Optional retention policy mart
MART_RETENTION_ENABLED=false
MART_TOP_COINS_RETENTION_DAYS=90
MART_MARKET_SUMMARY_RETENTION_DAYS=0
MART_COIN_DAILY_RETENTION_DAYS=730
MART_COIN_ROLLING_RETENTION_DAYS=730

# Optional ops alerting
OPS_ALERT_LOOKBACK_HOURS=6
OPS_ALERT_MAX_ITEMS=20
OPS_ALERT_EXCLUDE_DAG_IDS=ops_alerting_dag

MYSQL_HOST=172.xx.xx.1
MYSQL_PORT=3306
MYSQL_USER=crypto_app
MYSQL_PASSWORD=your_mysql_password_here

MYSQL_DATABASE_RAW=crypto_raw
MYSQL_DATABASE_CORE=crypto_core
MYSQL_DATABASE_MART=crypto_mart
MYSQL_DATABASE_OPS=crypto_ops

AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False
```

Rule penting:
- jangan commit `.env`
- jangan hardcode secret di Python files
- jangan taruh secret di docs
- `.env.example` hanya berisi placeholder

Untuk runtime Airflow di Docker:
- gunakan `.env` sebagai base config
- gunakan `.env.docker` sebagai override container
  - `MYSQL_HOST=host.docker.internal`
  - `AIRFLOW_UID=1000` (hindari masalah permission volume mount logs)

---

## 8. Setup Lokal

### 8.1 Clone repository
```bash
git clone <your-repo-url>
cd crypto-data-platform
```

### 8.2 Buat virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 8.3 Install dependencies
```bash
pip install -r requirements.txt
```

### 8.4 Tambahan dependency untuk MySQL 8 auth
Repository ini memakai `cryptography` karena MySQL 8 modern auth methods membutuhkannya.

### 8.5 Buat `.env`
Copy `.env.example` menjadi `.env`, lalu isi nilainya sesuai local setup.

### 8.6 Inisialisasi database project
```bash
python3 scripts/init_db.py
```

### 8.7 Smoke test konektivitas dasar
```bash
python3 scripts/smoke_test.py
```

### 8.8 Jalankan ingestion lokal tanpa Airflow (fase awal v1)
```bash
# Flow coin list saja
python3 scripts/run_local_extract.py --flow coin_list

# Flow coin markets top 100 saja
python3 scripts/run_local_extract.py --flow coin_markets

# Flow global metrics saja
python3 scripts/run_local_extract.py --flow global

# Flow coin history (sample top 5, 1 hari) untuk validasi cepat
python3 scripts/run_local_extract.py --flow coin_history --history-top-n 5 --history-days 1

# Jalankan semua flow raw yang tersedia
python3 scripts/run_local_extract.py --flow all
```

### 8.9 Start Airflow
Setelah setup local extract berhasil, jalankan Airflow:

```bash
make airflow-init
make up
make ps
```

Airflow UI:
- URL: `http://localhost:8080`
- user: `admin`
- password: `admin`

Untuk stop:

```bash
make down
```

Opsional dari Windows host (tanpa shell WSL), gunakan wrapper:

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

Panduan run yang lebih lengkap ada di `docs/how_to_run.md`.
Runbook operasional (backfill/rerun/failure handling) ada di `docs/runbook.md`.
Status flow implementasi terkini ada di `docs/pipeline_flow.md`.

---

## 9. DAG Utama

### `reference_coin_list_dag`
Implementasi saat ini:
- extract `/coins/list`
- write raw JSON ke `data/raw/coins_list/`
- load append ke `crypto_raw.raw_coin_list_snapshot`
- log `pipeline_run` dan `task_run` ke `crypto_ops`
- run DQ raw + core (structural + business), lalu simpan hasil ke `crypto_ops.dq_check_result`
- merge/upsert ke `crypto_core.dim_coin`

### `market_snapshot_hourly_dag`
Implementasi saat ini:
- extract `/coins/markets` untuk top 100 (`TRACKED_COINS_TOP_N`)
- write raw JSON ke `data/raw/coins_markets/`
- load append ke `crypto_raw.raw_coin_markets_snapshot`
- log `pipeline_run` dan `task_run` ke `crypto_ops`
- run DQ raw + core + mart (structural + business), lalu simpan hasil ke `crypto_ops.dq_check_result`
- merge/upsert ke `crypto_core.dim_coin`
- merge/upsert ke `crypto_core.fact_coin_market_snapshot`
- refresh `crypto_mart.mart_top_coins_latest`
- apply retention cleanup untuk `mart_top_coins_latest` (env-driven)

### `global_market_hourly_dag`
Implementasi saat ini:
- extract `/global`
- write raw JSON ke `data/raw/global/`
- load append ke `crypto_raw.raw_global_snapshot`
- log `pipeline_run` dan `task_run` ke `crypto_ops`
- run DQ raw + core + mart (structural + business), lalu simpan hasil ke `crypto_ops.dq_check_result`
- merge/upsert ke `crypto_core.fact_global_market`
- refresh `crypto_mart.mart_market_summary_daily`
- apply retention cleanup untuk `mart_market_summary_daily` (env-driven)

### `coin_history_daily_dag`
Implementasi saat ini:
- extract `/coins/{id}/market_chart` untuk tracked coins (default: `TRACKED_COINS_TOP_N`)
- write raw JSON per coin ke `data/raw/coin_market_chart/`
- load append ke `crypto_raw.raw_coin_market_chart_point`
- log `pipeline_run` dan `task_run` ke `crypto_ops`
- run DQ raw + core + mart (structural + business), lalu simpan hasil ke `crypto_ops.dq_check_result`
- merge/upsert ke `crypto_core.fact_coin_price_history`
- refresh `crypto_mart.mart_coin_daily_metrics`
- refresh `crypto_mart.mart_coin_rolling_metrics`
- apply retention cleanup untuk `mart_coin_daily_metrics` dan `mart_coin_rolling_metrics` (env-driven)

Catatan:
- Merge core menggunakan pola `INSERT ... ON DUPLICATE KEY UPDATE`, jadi `source_batch_id` pada tabel fact bisa tertimpa oleh run yang lebih baru di grain yang sama.
- Flow `coin_history` mendukung tuning throttling/retry via env atau `dag_run.conf`:
  - `batch_size`
  - `request_pause_seconds`
  - `batch_pause_seconds`
  - `client_max_retries`
  - `client_backoff_seconds`

### `ops_alerting_dag`
Implementasi saat ini:
- schedule setiap 15 menit
- baca `crypto_ops.pipeline_run` dan `crypto_ops.dq_check_result`
- fail task jika ada `pipeline_run` failed atau DQ fail pada lookback window
- konfigurasi via `OPS_ALERT_*`

---

## 10. Data Model Overview

### Database
- `crypto_raw`
- `crypto_core`
- `crypto_mart`
- `crypto_ops`

### Tabel core utama
- `dim_coin`
- `fact_coin_market_snapshot`
- `fact_coin_price_history`
- `fact_global_market`

### Tabel mart utama
- `mart_coin_daily_metrics`
- `mart_coin_rolling_metrics`
- `mart_top_coins_latest`
- `mart_market_summary_daily`

### Tabel ops utama
- `pipeline_run`
- `task_run`
- `api_request_log`
- `dq_check_result`
- `ingestion_watermark`
- `quarantine_bad_record`

---

## 11. Data Quality

Basic quality controls:
- key columns tidak boleh null
- metric numerik harus valid
- duplicate grain checks
- row count sanity checks
- quarantine flow untuk record bermasalah jika perlu
- business rule checks layer core/mart (contoh: rank top coin harus positif, persentase market cap BTC/ETH berada di range 0..100, dan rolling metric tidak boleh negatif)

Prinsipnya: lebih baik log error dengan jelas daripada diam-diam skip tanpa jejak.

---

## 12. Observability

Pipeline mencatat:
- run metadata
- request metadata
- row counts
- retry counts
- failures
- hasil DQ

Logs disimpan di:
- file/application logs
- MySQL ops tables

Ini penting supaya proyekmu terlihat seperti sistem yang dirancang, bukan script yang beruntung.

---

## 13. Workflow Development

Cara kerja yang disarankan:
1. mulai dari `README.md`, `docs/final_architecture.md`, dan `docs/runbook.md`
2. kerjakan perubahan kecil, aman, dan mudah direview
3. validasi perubahan dengan script/test sebelum commit
4. pisahkan perubahan fungsional dari perubahan dokumentasi bila memungkinkan
5. gunakan akses database langsung hanya untuk inspeksi/validasi, bukan sebagai dependency runtime

---

## 14. Mengapa Proyek Ini Relevan untuk Portfolio

Proyek ini menunjukkan kesiapan untuk pekerjaan data engineering, terutama dalam:
- ingestion pipeline design
- workflow orchestration
- SQL dan data modeling
- local environment discipline
- debugging dan observability
- secure configuration handling

Ini jauh lebih kuat daripada notebook atau script tunggal.

---

## 15. Future Enhancements

Kemungkinan v2:
- dbt migration setelah warehouse berganti
- dashboard layer
- alerting
- cloud deployment
- CI/CD
- lebih banyak source system
- data quality framework yang lebih kuat

---

## 16. Lisensi dan Penggunaan

Repository ini ditujukan untuk pembelajaran, demonstrasi portfolio, dan latihan engineering.
Jangan commit secret atau credential sensitif.
