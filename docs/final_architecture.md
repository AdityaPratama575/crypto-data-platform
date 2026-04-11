# Final Architecture - Crypto Data Platform

## 1. Tujuan Proyek

Proyek ini adalah platform batch data engineering untuk portfolio yang mengambil data market cryptocurrency dari CoinGecko Demo API, menyimpan data mentah dan data terstruktur ke MySQL lokal, lalu menyiapkan tabel analytics-ready untuk kebutuhan analisis lanjutan.

Fokus utama proyek ini bukan dashboard. Fokus utama proyek ini adalah menunjukkan kemampuan data engineering yang benar-benar relevan untuk dunia kerja, yaitu:

- API ingestion
- orchestration dengan Airflow
- layered data modeling
- idempotent loading
- data quality control
- observability
- local development yang reproducible
- secure configuration handling

---

## 2. Prinsip Arsitektur

Arsitektur proyek ini mengikuti prinsip berikut:

1. Pipeline dulu, dashboard belakangan
2. Pisahkan dengan jelas layer raw, core, mart, dan ops
3. Job harus rerunnable dan aman diulang
4. Semua timestamp disimpan dalam UTC
5. Raw payload harus tetap disimpan untuk audit, replay, dan debugging
6. Secret tidak boleh di-hardcode
7. Repository harus tetap bisa berjalan tanpa MCP
8. MCP boleh dipakai developer sebagai akselerator development, bukan dependency utama runtime
9. Struktur repo harus mudah dipahami reviewer dan recruiter

---

## 3. Sumber Data

### Source API
CoinGecko Demo API

### Endpoint utama
- `/coins/list`
- `/coins/markets`
- `/coins/{id}/market_chart`
- `/global`

### Alasan pemilihan endpoint
- `/coins/list` dipakai untuk master/reference data coin
- `/coins/markets` dipakai untuk snapshot market periodik
- `/coins/{id}/market_chart` dipakai untuk history time series coin terpilih
- `/global` dipakai untuk ringkasan market crypto secara keseluruhan

### Constraint penting
- API key wajib dibaca dari environment variable
- API usage harus memperhatikan rate limit
- Historical sync harus dibatasi oleh tracked coin policy
- Proyek ini memakai tracked coin policy: **Top 100 coins**

---

## 4. High-Level Architecture

```text
CoinGecko Demo API
        |
        v
Python Extract Layer
(auth, retry, timeout, pagination, validation, rate-limit awareness)
        |
        +------------------------------+
        |                              |
        v                              v
Local Raw JSON Landing Zone            MySQL crypto_ops
(data/raw/...)                         (run metadata, logs, DQ, watermarks)
        |
        v
MySQL crypto_raw
(raw snapshots and flattened raw tables)
        |
        v
MySQL crypto_core
(cleaned and normalized warehouse-style tables)
        |
        v
MySQL crypto_mart
(analytics-ready marts)
        |
        v
Future BI / dashboard layer
(not part of v1)
```

Airflow menjadi orchestrator untuk seluruh pipeline.

---

## 5. Layer Data

## 5.1 `crypto_raw`
Tujuan:
- menyimpan hasil ingestion mentah
- menyimpan payload asli dari API
- memungkinkan replay, audit, dan debugging

Contoh tabel:
- `raw_coin_list_snapshot`
- `raw_coin_markets_snapshot`
- `raw_coin_market_chart_point`
- `raw_global_snapshot`

Kolom umum:
- `batch_id`
- `dag_run_id`
- `task_run_id`
- `logical_run_ts_utc`
- `extracted_at_utc`
- `source_endpoint`
- `request_params_json`
- `raw_payload_json`

## 5.2 `crypto_core`
Tujuan:
- membersihkan data
- menormalisasi facts dan dimensions
- menjadi layer warehouse utama untuk transformasi lanjutan

Contoh tabel:
- `dim_coin`
- `fact_coin_market_snapshot`
- `fact_coin_price_history`
- `fact_global_market`

## 5.3 `crypto_mart`
Tujuan:
- menyediakan tabel yang siap dipakai analisis
- mempermudah pembuatan dashboard di fase berikutnya

Contoh tabel:
- `mart_coin_daily_metrics`
- `mart_coin_rolling_metrics`
- `mart_top_coins_latest`
- `mart_market_summary_daily`

## 5.4 `crypto_ops`
Tujuan:
- observability
- logging operasional
- data quality result tracking
- ingestion watermark tracking
- quarantine record bermasalah

Contoh tabel:
- `pipeline_run`
- `task_run`
- `api_request_log`
- `dq_check_result`
- `ingestion_watermark`
- `quarantine_bad_record`

---

## 6. Grain dan Model Data

### 6.1 Grain tabel utama

#### `fact_coin_market_snapshot`
Satu baris per:
- `snapshot_ts_utc`
- `coin_id`
- `vs_currency`

#### `fact_coin_price_history`
Satu baris per:
- `price_ts_utc`
- `coin_id`
- `vs_currency`

#### `fact_global_market`
Satu baris per:
- `snapshot_ts_utc`

### 6.2 Keputusan model data
- `coin_id` dari CoinGecko dipakai sebagai business identifier utama
- semua timestamp disimpan dalam UTC
- ingestion timestamp dipisah dari source logical timestamp
- raw layer bersifat append-first
- core dan mart layer boleh memakai merge/upsert logic

---

## 7. Ingestion Design

### 7.1 Tanggung jawab extract layer
Extract layer bertanggung jawab untuk:
- HTTP request
- autentikasi via environment variable
- retry dengan backoff
- timeout handling
- response validation
- pagination
- penyimpanan payload mentah ke local raw landing zone
- pengiriman batch ke raw table loader

### 7.2 Load strategy
- raw layer: append-first
- core layer: deterministic merge/upsert
- mart layer: rebuild atau merge sesuai grain tabel

### 7.3 Tracked coin policy
Historical ingestion **tidak** dijalankan ke semua coin.

Untuk v1, proyek ini memakai:
- `TRACKED_COINS_MODE=top_n`
- `TRACKED_COINS_TOP_N=100`

Tujuannya:
- mengontrol call budget API
- menjaga pipeline tetap realistis
- fokus pada subset coin yang meaningful untuk portfolio

---

## 8. Orchestration Design

Airflow dipakai sebagai orchestrator.

### 8.1 DAG utama

#### `reference_coin_list_dag`
Tujuan:
- sync master/reference coin list

Frekuensi:
- daily

Task utama:
- extract coin list
- write raw JSON
- load raw table
- merge ke `dim_coin`
- log metadata run

#### `market_snapshot_hourly_dag`
Tujuan:
- ambil snapshot market untuk top 100 coins

Frekuensi:
- hourly

Task utama:
- extract market snapshots
- write raw JSON
- load raw snapshot table
- build/update `fact_coin_market_snapshot`
- refresh latest mart tables
- run DQ checks
- log run metadata

#### `global_market_hourly_dag`
Tujuan:
- ambil global market metrics

Frekuensi:
- hourly

Task utama:
- extract global metrics
- write raw JSON
- load raw table
- update `fact_global_market`
- refresh market summary mart
- log run metadata

#### `coin_history_daily_dag`
Tujuan:
- ambil history harian untuk tracked coins

Frekuensi:
- daily

Task utama:
- read watermark
- fetch missing history window
- write raw JSON
- load raw history table
- merge ke `fact_coin_price_history`
- advance watermark
- run DQ checks
- log run metadata

### 8.2 Manual runner baseline (tanpa Airflow)

Untuk fase implementasi awal v1, tersedia jalur eksekusi manual lokal sebelum wiring DAG penuh:

- `scripts/run_local_extract.py --flow coin_list`
  - memanggil endpoint `/coins/list`
  - menyimpan raw envelope JSON ke `data/raw/coins_list/`
  - append ke `crypto_raw.raw_coin_list_snapshot`
- `scripts/run_local_extract.py --flow coin_markets`
  - memanggil endpoint `/coins/markets` untuk top 100 (`TRACKED_COINS_TOP_N`)
  - menyimpan raw envelope JSON ke `data/raw/coins_markets/`
  - append ke `crypto_raw.raw_coin_markets_snapshot`
- `scripts/run_local_extract.py --flow global`
  - memanggil endpoint `/global`
  - menyimpan raw envelope JSON ke `data/raw/global/`
  - append ke `crypto_raw.raw_global_snapshot`
- `scripts/run_local_extract.py --flow coin_history --history-top-n <N> --history-days <D>`
  - memilih tracked coins via `/coins/markets` (top N)
  - memanggil endpoint `/coins/{id}/market_chart`
  - menyimpan raw envelope JSON per coin ke `data/raw/coin_market_chart/`
  - flatten append ke `crypto_raw.raw_coin_market_chart_point`

Tujuan jalur manual ini:
- validasi end-to-end extract dan load lebih cepat
- menjaga business logic tetap di `src/`
- mempermudah debugging sebelum orchestration Airflow diaktifkan

### 8.3 Baseline DAG yang sudah diimplementasikan

Saat ini fase awal orchestration sudah memiliki:

- `dags/reference_coin_list_dag.py`
  - schedule: daily (`0 2 * * *`)
  - task baseline: extract `/coins/list`, write raw JSON, load `raw_coin_list_snapshot`, merge ke `crypto_core.dim_coin`
  - baseline observability: tulis `pipeline_run` + `task_run` ke `crypto_ops`
  - baseline DQ: validasi raw + core (structural + business) dan simpan hasil ke `crypto_ops.dq_check_result`
- `dags/market_snapshot_hourly_dag.py`
  - schedule: hourly (`5 * * * *`)
  - task baseline: extract `/coins/markets`, write raw JSON, load `raw_coin_markets_snapshot`
  - merge baseline: upsert `crypto_core.dim_coin` dan `crypto_core.fact_coin_market_snapshot`
  - refresh mart: upsert `crypto_mart.mart_top_coins_latest`
  - cleanup mart: retention cleanup `mart_top_coins_latest` (env-driven)
  - baseline observability: tulis `pipeline_run` + `task_run` ke `crypto_ops`
  - baseline DQ: validasi raw + core + mart (structural + business) dan simpan hasil ke `crypto_ops.dq_check_result`
- `dags/global_market_hourly_dag.py`
  - schedule: hourly (`10 * * * *`)
  - task baseline: extract `/global`, write raw JSON, load `raw_global_snapshot`, merge ke `crypto_core.fact_global_market`
  - refresh mart: upsert `crypto_mart.mart_market_summary_daily`
  - cleanup mart: retention cleanup `mart_market_summary_daily` (env-driven)
  - baseline observability: tulis `pipeline_run` + `task_run` ke `crypto_ops`
  - baseline DQ: validasi raw + core + mart (structural + business) dan simpan hasil ke `crypto_ops.dq_check_result`
- `dags/coin_history_daily_dag.py`
  - schedule: daily (`15 3 * * *`)
  - task baseline: extract `/coins/{id}/market_chart`, write raw JSON per coin, load `raw_coin_market_chart_point`, merge ke `crypto_core.fact_coin_price_history`
  - refresh mart: upsert `crypto_mart.mart_coin_daily_metrics` dan `crypto_mart.mart_coin_rolling_metrics`
  - cleanup mart: retention cleanup `mart_coin_daily_metrics` dan `mart_coin_rolling_metrics` (env-driven)
  - baseline observability: tulis `pipeline_run` + `task_run` ke `crypto_ops`
  - baseline DQ: validasi raw + core + mart (structural + business) dan simpan hasil ke `crypto_ops.dq_check_result`
  - hardening history: dukung throttling/batching/retry via env dan `dag_run.conf`, plus preservasi presisi millisecond timestamp saat load raw
- `dags/ops_alerting_dag.py`
  - schedule: setiap 15 menit (`*/15 * * * *`)
  - cek `crypto_ops.pipeline_run` dan `crypto_ops.dq_check_result`
  - fail task bila ada `pipeline_run` failed atau DQ fail pada lookback window

Catatan implementasi merge:
- Query merge berjalan dari koneksi `crypto_core` dengan referensi tabel raw fully-qualified (`crypto_raw.raw_*`) untuk menjaga cross-database query tetap valid.

Pendekatan ini menjaga rollout tetap kecil dan aman sambil memberi jalur transisi dari runner manual ke Airflow.

---

## 9. Scheduling Strategy

Jadwal yang direkomendasikan untuk v1:

- coin list sync: daily
- market snapshot: hourly
- global snapshot: hourly
- history sync: daily
- marts refresh: setelah upstream selesai

Catatan:
- history sync dijalankan setelah data hari UTC sebelumnya aman tersedia
- semua DAG harus aman diulang
- catchup harus diatur eksplisit, bukan dibiarkan default lalu bikin kekacauan

---

## 10. Data Quality Rules

Minimum DQ checks:

### Raw checks
- payload ada
- response status tercatat
- endpoint metadata tercatat

### Core checks
- `coin_id` tidak boleh null
- `snapshot_ts_utc` tidak boleh null
- metric numerik harus valid numerik
- duplicate key pada grain fact harus ditolak atau dikarantina
- business checks per tabel (contoh):
  - `dim_coin`: `first_seen_at_utc <= last_seen_at_utc`, `is_active` hanya 0/1, nama coin tidak blank
  - `fact_coin_market_snapshot`: `market_cap_rank > 0`, `current_price/market_cap/total_volume >= 0`
  - `fact_global_market`: `market_cap_pct_btc` dan `market_cap_pct_eth` di range 0..100
  - `fact_coin_price_history`: `price/market_cap/volume >= 0`, `vs_currency` konsisten

### Mart checks
- tidak boleh ada duplicate business key
- rolling metric hanya dihitung saat lookback minimum tersedia
- ranking top coin harus deterministic
- business checks per tabel (contoh):
  - `mart_top_coins_latest`: jumlah row latest snapshot memenuhi `TRACKED_COINS_TOP_N`, rank latest berada di range valid
  - `mart_market_summary_daily`: `total_market_cap` dan `total_volume` non-negatif, persen BTC/ETH di range 0..100
  - `mart_coin_daily_metrics`: `close_price` berada di antara `min_price` dan `max_price`
  - `mart_coin_rolling_metrics`: rolling averages dan rolling volume tidak negatif

### Failure policy
- jangan diam-diam membuang bad rows
- karantina record bermasalah jika memungkinkan
- log issue ke `crypto_ops`
- fail task hanya kalau integritas data benar-benar terancam

---

## 11. Logging dan Observability

Sistem harus mencatat:
- DAG run ID
- task run ID
- execution timestamps
- endpoint yang dipanggil
- request params
- response status
- row count extracted
- row count loaded
- row count merged
- retry count
- error details
- summary DQ result

Logging ada di dua tempat:
- application/file logs
- MySQL `crypto_ops`

Tujuannya agar pipeline bisa di-debug dari Airflow maupun dari SQL.

---

## 12. Secrets dan Configuration

### Rule utama
Secret **tidak boleh** disimpan di:
- source code yang di-track Git
- README
- architecture docs
- SQL files
- notebook output

### Cara yang benar
Pakai:
- `.env` untuk local development
- `.env.example` untuk placeholder
- `os.environ` di Python
- `.env.docker` untuk override runtime container Docker (contoh: host MySQL)

### Environment variable utama
- `COINGECKO_API_KEY`
- `COINGECKO_BASE_URL`
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE_RAW`
- `MYSQL_DATABASE_CORE`
- `MYSQL_DATABASE_MART`
- `MYSQL_DATABASE_OPS`
- `TRACKED_COINS_TOP_N`

Optional tuning untuk flow history:
- `COIN_HISTORY_BATCH_SIZE`
- `COIN_HISTORY_REQUEST_PAUSE_SECONDS`
- `COIN_HISTORY_BATCH_PAUSE_SECONDS`
- `COIN_HISTORY_MAX_RETRIES`
- `COIN_HISTORY_BACKOFF_SECONDS`

Optional retention policy untuk mart:
- `MART_RETENTION_ENABLED`
- `MART_TOP_COINS_RETENTION_DAYS`
- `MART_MARKET_SUMMARY_RETENTION_DAYS`
- `MART_COIN_DAILY_RETENTION_DAYS`
- `MART_COIN_ROLLING_RETENTION_DAYS`

Optional ops alerting:
- `OPS_ALERT_LOOKBACK_HOURS`
- `OPS_ALERT_MAX_ITEMS`
- `OPS_ALERT_EXCLUDE_DAG_IDS`

Untuk deployment Airflow via Docker Compose di setup WSL + Windows host:
- `.env` tetap dipakai sebagai source utama
- `.env.docker` override `MYSQL_HOST` menjadi `host.docker.internal`
- `.env.docker` juga bisa mengatur `AIRFLOW_UID` agar volume mount writable

### Catatan MCP
Jika MCP MySQL tersedia di VS Code, developer boleh memakainya untuk:
- inspeksi schema
- menjalankan DDL
- validasi row count
- sanity check data

Tetapi:
- repository code tidak boleh bergantung pada MCP
- semua perubahan penting tetap harus ditulis ke file repo

---

## 13. Security dan Safety Constraints

Tim engineering harus mengikuti rule berikut:
- jangan pernah print secret ke output
- jangan pernah hardcode API key
- jangan pernah commit `.env`
- jangan pernah drop/truncate/delete data tanpa approval eksplisit
- preview destructive SQL sebelum dijalankan
- utamakan perubahan yang reproducible dan tercatat di repo

---

## 14. Repository Design

Repository harus dipisah berdasarkan tanggung jawab:
- `docs/` untuk dokumentasi
  - termasuk `pipeline_flow.md` untuk status implementasi flow
- `dags/` untuk Airflow DAGs
- `src/` untuk Python code
- `sql/ddl/` untuk schema DDL
- `sql/dml/` untuk merge/transform SQL
- `sql/quality/` untuk DQ SQL
- `scripts/` untuk utility scripts
- `tests/` untuk tests
- `data/raw/` untuk landing zone payload mentah

---

## 15. Non-Goals untuk v1

Hal berikut **di luar scope v1**:
- streaming / Kafka
- cloud warehouse migration
- dashboard implementation
- dbt adoption
- multi-source ingestion
- production deployment ke cloud
- Kubernetes

Semua itu boleh jadi v2, bukan alasan untuk mengacaukan v1.

---

## 16. Mengapa Arsitektur Ini Bagus untuk Portfolio

Desain ini menunjukkan kemampuan yang relevan untuk karir data engineering:
- API ingestion engineering
- orchestration dengan Airflow
- layered warehouse thinking
- operational logging
- data quality handling
- reproducibility
- secure secret handling
- local infrastructure pragmatism

Ini jauh lebih kuat daripada proyek satu script yang hanya fetch API lalu insert ke satu tabel.

---

## 17. Scope Statement Final

Proyek ini adalah local, batch-oriented, portfolio-grade crypto data platform yang dibangun dengan Python, Airflow, MySQL, Docker, dan layered warehouse design. Prioritas utamanya adalah ingestion reliability, structured storage, observability, dan extensibility, bukan dashboarding atau kompleksitas palsu.
