# Crypto Data Platform

A batch-oriented data engineering portfolio project that ingests cryptocurrency market data from the CoinGecko Demo API into local MySQL using Python and Airflow.

This repository focuses on pipeline design, data modeling, observability, and clean, reproducible local development.

---

## 1. Project Goals

The goal of this project is to demonstrate practical data engineering skills through an end-to-end ETL/ELT-style batch pipeline.

Core capabilities demonstrated:
- extracting data from an external API
- orchestrating workflows with Airflow
- storing both raw and structured data in layered MySQL databases
- preserving raw payloads for replay and debugging
- implementing basic data quality checks
- producing analytics-ready mart tables
- keeping local development secure and reproducible

This repository intentionally does **not** include a dashboard in v1.

---

## 2. Architecture Summary

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

Airflow orchestrates the main ingestion and transformation jobs.

---

## 3. Tech Stack

- Python
- Apache Airflow
- MySQL 8
- Docker / Docker Compose
- SQL
- dotenv / environment variables
- Pytest

Optional local development tools:
- VS Code
- MySQL client (CLI/Workbench/DBeaver)

---

## 4. Project Scope

### In scope for v1
- CoinGecko Demo API ingestion
- raw JSON landing zone
- raw/core/mart/ops database layers
- Airflow DAGs
- SQL transformations
- data quality checks
- operational logging
- local development setup

### Out of scope for v1
- BI dashboard
- dbt
- cloud warehouse
- Kafka / streaming
- production cloud deployment

---

## 5. Core Data Domains

### Reference data
- coin master/reference list

### Snapshot data
- current price
- market cap
- market rank
- volume
- price change

### Historical data
- time series for tracked coins

### Global market data
- overall crypto market summary

---

## 6. Repository Structure

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

Create a local `.env` file from `.env.example`.

Example:

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

# Optional tuning for coin_history_daily_dag
COIN_HISTORY_BATCH_SIZE=10
COIN_HISTORY_REQUEST_PAUSE_SECONDS=1.5
COIN_HISTORY_BATCH_PAUSE_SECONDS=8.0
COIN_HISTORY_MAX_RETRIES=6
COIN_HISTORY_BACKOFF_SECONDS=4

# Optional mart retention policy
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

Important rules:
- do not commit `.env`
- do not hardcode secrets in Python files
- do not put secrets in documentation
- keep `.env.example` as placeholders only

For Airflow runtime in Docker:
- use `.env` as the base config
- use `.env.docker` as container override
  - `MYSQL_HOST=host.docker.internal`
  - `AIRFLOW_UID=1000` (to avoid volume mount permission issues)

---

## 8. Local Setup

### 8.1 Clone repository
```bash
git clone <your-repo-url>
cd crypto-data-platform
```

### 8.2 Create virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 8.3 Install dependencies
```bash
pip install -r requirements.txt
```

### 8.4 Extra dependency for MySQL 8 auth
This repository uses `cryptography` because modern MySQL 8 auth methods require it.

### 8.5 Create `.env`
Copy `.env.example` to `.env`, then fill in values based on your local setup.

### 8.6 Initialize project databases
```bash
python3 scripts/init_db.py
```

### 8.7 Run basic connectivity smoke test
```bash
python3 scripts/smoke_test.py
```

### 8.8 Run local ingestion without Airflow (early v1 phase)
```bash
# Coin list flow only
python3 scripts/run_local_extract.py --flow coin_list

# Coin markets top 100 flow only
python3 scripts/run_local_extract.py --flow coin_markets

# Global metrics flow only
python3 scripts/run_local_extract.py --flow global

# Coin history flow (top 5 sample, 1 day) for quick validation
python3 scripts/run_local_extract.py --flow coin_history --history-top-n 5 --history-days 1

# Run all available raw flows
python3 scripts/run_local_extract.py --flow all
```

### 8.9 Start Airflow
After local extract setup is successful, start Airflow:

```bash
make airflow-init
make up
make ps
```

Airflow UI:
- URL: `http://localhost:8080`
- user: `admin`
- password: `admin`

To stop:

```bash
make down
```

Optional from Windows host (without WSL shell), use wrappers:

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

More complete runtime guidance is available in `docs/how_to_run.md`.
Operational backfill/rerun/failure handling is in `docs/runbook.md`.
Current implementation status is in `docs/pipeline_flow.md`.

---

## 9. Main DAGs

### `reference_coin_list_dag`
Current implementation:
- extract `/coins/list`
- write raw JSON to `data/raw/coins_list/`
- append-load to `crypto_raw.raw_coin_list_snapshot`
- log `pipeline_run` and `task_run` to `crypto_ops`
- run raw + core DQ checks (structural + business) and save to `crypto_ops.dq_check_result`
- merge/upsert into `crypto_core.dim_coin`

### `market_snapshot_hourly_dag`
Current implementation:
- extract `/coins/markets` for top 100 (`TRACKED_COINS_TOP_N`)
- write raw JSON to `data/raw/coins_markets/`
- append-load to `crypto_raw.raw_coin_markets_snapshot`
- log `pipeline_run` and `task_run` to `crypto_ops`
- run raw + core + mart DQ checks (structural + business) and save to `crypto_ops.dq_check_result`
- merge/upsert into `crypto_core.dim_coin`
- merge/upsert into `crypto_core.fact_coin_market_snapshot`
- refresh `crypto_mart.mart_top_coins_latest`
- apply retention cleanup for `mart_top_coins_latest` (env-driven)

### `global_market_hourly_dag`
Current implementation:
- extract `/global`
- write raw JSON to `data/raw/global/`
- append-load to `crypto_raw.raw_global_snapshot`
- log `pipeline_run` and `task_run` to `crypto_ops`
- run raw + core + mart DQ checks (structural + business) and save to `crypto_ops.dq_check_result`
- merge/upsert into `crypto_core.fact_global_market`
- refresh `crypto_mart.mart_market_summary_daily`
- apply retention cleanup for `mart_market_summary_daily` (env-driven)

### `coin_history_daily_dag`
Current implementation:
- extract `/coins/{id}/market_chart` for tracked coins (default: `TRACKED_COINS_TOP_N`)
- write raw JSON per coin to `data/raw/coin_market_chart/`
- append-load to `crypto_raw.raw_coin_market_chart_point`
- log `pipeline_run` and `task_run` to `crypto_ops`
- run raw + core + mart DQ checks (structural + business) and save to `crypto_ops.dq_check_result`
- merge/upsert into `crypto_core.fact_coin_price_history`
- refresh `crypto_mart.mart_coin_daily_metrics`
- refresh `crypto_mart.mart_coin_rolling_metrics`
- apply retention cleanup for `mart_coin_daily_metrics` and `mart_coin_rolling_metrics` (env-driven)

Notes:
- Core merge uses `INSERT ... ON DUPLICATE KEY UPDATE`, so `source_batch_id` in fact tables can be overwritten by newer runs on the same grain.
- The `coin_history` flow supports throttling/retry tuning via env or `dag_run.conf`:
  - `batch_size`
  - `request_pause_seconds`
  - `batch_pause_seconds`
  - `client_max_retries`
  - `client_backoff_seconds`

### `ops_alerting_dag`
Current implementation:
- schedule every 15 minutes
- read `crypto_ops.pipeline_run` and `crypto_ops.dq_check_result`
- fail task when there is a failed `pipeline_run` or failed DQ in the lookback window
- configurable via `OPS_ALERT_*`

---

## 10. Data Model Overview

### Databases
- `crypto_raw`
- `crypto_core`
- `crypto_mart`
- `crypto_ops`

### Main core tables
- `dim_coin`
- `fact_coin_market_snapshot`
- `fact_coin_price_history`
- `fact_global_market`

### Main mart tables
- `mart_coin_daily_metrics`
- `mart_coin_rolling_metrics`
- `mart_top_coins_latest`
- `mart_market_summary_daily`

### Main ops tables
- `pipeline_run`
- `task_run`
- `api_request_log`
- `dq_check_result`
- `ingestion_watermark`
- `quarantine_bad_record`

---

## 11. Data Quality

Basic quality controls:
- key columns must not be null
- numeric metrics must be valid
- duplicate grain checks
- row count sanity checks
- quarantine flow for problematic records when needed
- core/mart business rule checks (for example: top coin rank must be positive, BTC/ETH market cap percentage must be in range 0..100, and rolling metrics must not be negative)

Principle: clear failure signals are better than silently skipping data issues.

---

## 12. Observability

The pipeline records:
- run metadata
- request metadata
- row counts
- retry counts
- failures
- DQ results

Logs are stored in:
- file/application logs
- MySQL ops tables

This helps the project look and behave like a designed system, not a lucky script.

---

## 13. Development Workflow

Recommended way of working:
1. start from `README.md`, `docs/final_architecture.md`, and `docs/runbook.md`
2. implement small, safe, reviewable changes
3. validate changes with scripts/tests before committing
4. separate functional changes from documentation changes when possible
5. use direct database access for inspection/validation only, not as runtime dependency

---

## 14. Why This Project Is Portfolio-Relevant

This project demonstrates readiness for data engineering roles, especially in:
- ingestion pipeline design
- workflow orchestration
- SQL and data modeling
- local environment discipline
- debugging and observability
- secure configuration handling

This is much stronger than a single notebook or script.

---

## 15. Future Enhancements

Potential v2 improvements:
- dbt migration after warehouse transition
- dashboard layer
- alerting
- cloud deployment
- CI/CD
- additional source systems
- stronger data quality framework

---

## 16. License and Usage

This repository is intended for learning, portfolio demonstration, and engineering practice.
Do not commit secrets or sensitive credentials.
