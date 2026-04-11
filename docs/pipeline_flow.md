# Pipeline Flow Status

Dokumen ini mencatat status implementasi pipeline per flow agar progres mudah dilacak.

Terakhir diperbarui: 2026-04-06

---

## 1. Ringkasan Status

- Phase 1 (foundation package): selesai
- Phase 2 (manual extract/load semua endpoint raw v1): selesai
- Phase 3 (Airflow baseline DAG): selesai (4 DAG)
- Phase 4 (Airflow runtime validation end-to-end): selesai untuk DAG baseline saat ini
- Phase 5 (crypto_ops logging + DQ checks per DAG): selesai (baseline)
- Phase 6 (merge raw -> core per DAG): selesai
- Phase 7 (automated test `ops_writer` + `quality.checks`): selesai
- Phase 8 (DQ checks layer core/mart): selesai (structural + business rules)
- Phase 9 (mart refresh dari DAG): selesai
- Phase 10 (coin_history retry + batching hardening): selesai
- Phase 11 (mart retention/cleanup policy): selesai
- Phase 12 (ops alerting otomatis dari `crypto_ops`): selesai
- Phase 13 (runbook operasional backfill/rerun/failure): selesai
- Phase 14 (monitoring stabilitas history + tuning baseline): selesai (baseline)

---

## 2. Flow yang Sudah Jalan (Manual Runner)

Runner:
- `scripts/run_local_extract.py --flow coin_list`
- `scripts/run_local_extract.py --flow coin_markets`
- `scripts/run_local_extract.py --flow global`
- `scripts/run_local_extract.py --flow coin_history --history-top-n <N> --history-days <D>`
- `scripts/run_local_extract.py --flow all`

Status:
- `/coins/list` -> raw JSON -> `crypto_raw.raw_coin_list_snapshot`: selesai
- `/coins/markets` top 100 -> raw JSON -> `crypto_raw.raw_coin_markets_snapshot`: selesai
- `/global` -> raw JSON -> `crypto_raw.raw_global_snapshot`: selesai
- `/coins/{id}/market_chart` -> raw JSON per coin -> `crypto_raw.raw_coin_market_chart_point`: selesai

Catatan:
- Raw layer append-first, jadi total row di tabel bersifat akumulatif antar batch.

---

## 3. Flow Airflow Saat Ini

### 3.1 `reference_coin_list_dag`

File:
- `dags/reference_coin_list_dag.py`

Task saat ini:
- extract `/coins/list`
- write raw JSON ke `data/raw/coins_list/`
- load append ke `crypto_raw.raw_coin_list_snapshot`
- merge ke `crypto_core.dim_coin`

Yang belum:
- belum ada proses mart di DAG reference (sesuai scope flow reference)

### 3.2 `market_snapshot_hourly_dag`

File:
- `dags/market_snapshot_hourly_dag.py`

Task saat ini:
- extract `/coins/markets` top 100
- write raw JSON ke `data/raw/coins_markets/`
- load append ke `crypto_raw.raw_coin_markets_snapshot`
- merge ke `crypto_core.dim_coin`
- merge ke `crypto_core.fact_coin_market_snapshot`

Yang belum:
- pengaturan retention/cleanup untuk snapshot mart historis

### 3.3 `global_market_hourly_dag`

File:
- `dags/global_market_hourly_dag.py`

Task saat ini:
- extract `/global`
- write raw JSON ke `data/raw/global/`
- load append ke `crypto_raw.raw_global_snapshot`
- merge ke `crypto_core.fact_global_market`

Yang belum:
- pengaturan retention/cleanup untuk snapshot mart historis

### 3.4 `coin_history_daily_dag`

File:
- `dags/coin_history_daily_dag.py`

Task saat ini:
- extract `/coins/{id}/market_chart` untuk tracked coins
- write raw JSON per coin ke `data/raw/coin_market_chart/`
- load append ke `crypto_raw.raw_coin_market_chart_point`
- merge ke `crypto_core.fact_coin_price_history`

Yang belum:
- tuning lanjutan berbasis observasi rate-limit real traffic

### 3.5 `ops_alerting_dag`

File:
- `dags/ops_alerting_dag.py`

Task saat ini:
- cek `crypto_ops.pipeline_run` untuk `run_status='failed'`
- cek `crypto_ops.dq_check_result` untuk DQ fail
- fail task jika ada alert pada lookback window

Yang belum:
- integrasi kanal notifikasi eksternal (Slack/Email/Webhook)

---

## 4. Environment & Runtime Notes

Strategi env saat ini:
- `.env` untuk local script dari WSL/Ubuntu
- `.env.docker` untuk override runtime container Docker (khususnya `MYSQL_HOST=host.docker.internal`)

Docker Compose saat ini sudah membaca kedua file env tersebut.
Untuk kompatibilitas permission volume mount, `docker-compose.yml` sekarang memakai default
`AIRFLOW_UID=1000` jika env host tidak meng-overwrite nilai ini.

---

## 5. Windows Convenience Scripts (.bat)

Disediakan wrapper sederhana:
- `scripts/windows/airflow_init.bat`
- `scripts/windows/airflow_up.bat`
- `scripts/windows/airflow_down.bat`
- `scripts/windows/reference_coin_list_trigger.bat`
- `scripts/windows/market_snapshot_trigger.bat`
- `scripts/windows/global_market_trigger.bat`
- `scripts/windows/coin_history_trigger.bat`

Tujuan:
- mempermudah eksekusi dari Windows host tanpa harus mengetik command Docker berulang.

Catatan:
- script `.bat` adalah wrapper convenience, bukan pengganti source of truth (`Makefile` dan dokumentasi tetap acuan utama).

---

## 6. Runtime Validation (Latest)

Validasi terbaru (2026-04-06):
- `make airflow-init`: sukses
- `make up`: sukses
- `reference_coin_list_dag`: sukses (scheduled + manual run)
- `market_snapshot_hourly_dag`: sukses (scheduled + manual run)
- `global_market_hourly_dag`: sukses (scheduled + manual run)
- `coin_history_daily_dag`: sukses (manual run validasi)
- test otomatis: `.venv/bin/pytest -q` -> `8 passed`
- test otomatis terbaru setelah retention + alerting + load timestamp fix:
  - `PYTHONPATH=. .venv/bin/pytest -q tests/test_load_raw_tables.py tests/test_ops_alerts.py tests/test_mart_retention.py tests/test_quality_checks.py`
  - hasil: `14 passed`
- verifikasi `raw_coin_list_snapshot`: batch baru masuk dengan `row_count` sekitar 17k per batch (contoh terbaru: 17813)
- verifikasi `raw_coin_markets_snapshot`: batch baru masuk dengan `row_count=100` per batch
- verifikasi `raw_global_snapshot`: batch baru masuk dengan `row_count=1` per batch
- verifikasi `raw_coin_market_chart_point`: batch baru masuk dengan `row_count=1445`, `coin_count=5` (run conf validasi `top_n=5`, `days=1`)
- verifikasi `crypto_ops.pipeline_run`: run manual validasi tercatat status `success` untuk 4 DAG
- verifikasi `crypto_ops.task_run`: row extracted/loaded/merged tercatat per task
- verifikasi `crypto_ops.dq_check_result`: seluruh check baseline validasi terbaru berstatus `pass`
- validasi merge manual terbaru:
  - `mergefix_ref_20260405T145930` -> `rows_merged=17913`
  - `mergefix_mkt_20260405T145930` -> `rows_merged=100`
  - `mergefix_glb_20260405T145930` -> `rows_merged=1`
  - `mergefix_hist_20260405T145930` -> `rows_merged=1445`
- validasi DQ core/mart + mart refresh terbaru:
  - `dqmart_ref_20260406T101500` -> success; DQ pass: `raw_coin_list_snapshot`, `dim_coin`
  - `dqmart_mkt_20260406T101500` -> success; DQ pass: `raw_coin_markets_snapshot`, `fact_coin_market_snapshot`, `mart_top_coins_latest`
  - `dqmart_glb_20260406T101500` -> success; DQ pass: `raw_global_snapshot`, `fact_global_market`, `mart_market_summary_daily`
  - `dqmart_hist_20260406T101500` -> success; DQ pass: `raw_coin_market_chart_point`, `fact_coin_price_history`, `mart_coin_daily_metrics`, `mart_coin_rolling_metrics`
- validasi finalisasi DQ bisnis core/mart:
  - `bizdq_ref_20260406T152610` -> success; `14 checks pass`, `failed_rows=0`
  - `bizdq_mkt_20260406T152610` -> success; `26 checks pass`, `failed_rows=0`
  - `bizdq_glb_20260406T152610` -> success; `20 checks pass`, `failed_rows=0`
  - `bizdq_hist_20260406T152610` -> success; `32 checks pass`, `failed_rows=0`
  - contoh rule bisnis yang tervalidasi:
    - `first_seen_lte_last_seen` (core `dim_coin`)
    - `market_cap_rank_positive` (core `fact_coin_market_snapshot`)
    - `market_cap_pct_btc_in_range` (core/mart global)
    - `close_price_within_min_max` (mart `mart_coin_daily_metrics`)
    - `rolling_metrics_non_negative` (mart `mart_coin_rolling_metrics`)
- validasi monitoring stabilitas history berbasis `crypto_ops`:
  - command: `python scripts/monitor_coin_history_stability.py --lookback-days 7 --max-rows 200`
  - hasil terbaru: `total_runs=8`, `success_runs=6`, `failed_runs=2`, `success_rate=75.00%`
  - kategori failure terlihat: `rate_limit_429`, `dq_raw_failure` (historical runs lama), `schema_or_table_missing` (historical run lama)
  - baseline tuning direkomendasikan:
    - `COIN_HISTORY_BATCH_SIZE=10`
    - `COIN_HISTORY_REQUEST_PAUSE_SECONDS=1.5`
    - `COIN_HISTORY_BATCH_PAUSE_SECONDS=8.0`
    - `COIN_HISTORY_MAX_RETRIES=6`
    - `COIN_HISTORY_BACKOFF_SECONDS=4`
- validasi rerun top 100 pasca fix timestamp + tuning:
  - run id: `tunehist_20260406T0900` -> `success`
  - `task_run`: `rows_extracted=100`, `rows_loaded=27740`, `rows_merged=27742`
  - `dq_check_result`: `32 checks pass`, `failed_rows=0`
  - indikator: anomali duplicate raw history pada run lama tidak muncul pada run validasi ini
- validasi alerting otomatis dari `crypto_ops`:
  - command: `python scripts/check_ops_alerts.py --lookback-hours 24 --exclude-dag-id ops_alerting_dag`
  - hasil: alert terdeteksi sesuai ekspektasi (mendeteksi `pipeline_run failed` + `dq fail`)
  - trigger DAG manual: `opsalert_20260406T0910` -> `failed` sesuai ekspektasi karena alert aktif pada window observasi
  - status DAG import: `airflow dags list-import-errors` -> `No data found`

Catatan runtime coin history:
- Run terjadwal default top 100 saat ini sempat `up_for_retry` karena CoinGecko `429` rate limit.
- Manual validasi dengan conf kecil (`top_n=5`) terbukti sukses end-to-end.
- Root cause merge failure sebelumnya:
  - query merge raw->core memakai nama tabel raw tanpa schema prefix
  - saat dieksekusi di koneksi `crypto_core`, MySQL membaca `crypto_core.raw_*` dan error `Table doesn't exist`
  - sudah diperbaiki dengan referensi fully-qualified `crypto_raw.raw_*`
- Catatan upsert core:
  - tabel fact memakai `ON DUPLICATE KEY UPDATE`
  - `source_batch_id` bisa berubah mengikuti run terbaru pada grain yang sama
- Catatan rowcount merge:
  - `cursor.rowcount` pada `INSERT ... ON DUPLICATE KEY UPDATE` bisa lebih besar dari jumlah row source karena update dihitung terpisah oleh MySQL

Batch terbaru yang terverifikasi:
- `coin_list_20260404T133157_bdd3203a`
- `coin_list_20260404T133142_d7bb683b`
- `global_20260404T140507_5e360753` (row_count=1)
- `coin_markets_20260404T143815_d7622111` (row_count=100)
- `coin_markets_20260404T143815_0b80065a` (row_count=100)
- `global_20260404T152510_b28d23ad` (row_count=1)
- `global_20260404T152506_3961419e` (row_count=1)
- `coin_history_20260404T153054_85b898d0` (row_count=1445, coin_count=5)
- `opsdq_ref_20260405T093200` (`pipeline_run=success`, `dq_pass=5 checks`)
- `opsdq_mkt_20260405T093200` (`pipeline_run=success`, `dq_pass=6 checks`)
- `opsdq_glb_20260405T093200` (`pipeline_run=success`, `dq_pass=5 checks`)
- `opsdq_hist_20260405T093200` (`pipeline_run=success`, `dq_pass=6 checks`)
- `coin_list_20260405T093220_7150cf37` (row_count=17813)
- `coin_markets_20260405T093224_24190c75` (row_count=100)
- `global_20260405T093227_4b44aca2` (row_count=1)
- `coin_history_20260405T093232_56790833` (row_count=1445, coin_count=5)
- `coin_list_20260405T145937_efe75863` (row_count=17813; run=`mergefix_ref_20260405T145930`)
- `coin_markets_20260405T145935_bfc2580b` (row_count=100; run=`mergefix_mkt_20260405T145930`)
- `global_20260405T145936_512e2a01` (row_count=1; run=`mergefix_glb_20260405T145930`)
- `coin_history_20260405T145938_406d13f9` (row_count=1445, coin_count=5; run=`mergefix_hist_20260405T145930`)
- `coin_list_20260406T080222_51f116e4` (row_count=17814; run=`dqmart_ref_20260406T101500`)
- `coin_markets_20260406T080221_91abd0c7` (row_count=100; run=`dqmart_mkt_20260406T101500`)
- `global_20260406T080221_72253d62` (row_count=1; run=`dqmart_glb_20260406T101500`)
- `coin_history_20260406T080223_1578102d` (row_count=1445, coin_count=5; run=`dqmart_hist_20260406T101500`)
- `bizdq_ref_20260406T152610` (`pipeline_run=success`, `dq_pass=14`, `dq_fail=0`)
- `bizdq_mkt_20260406T152610` (`pipeline_run=success`, `dq_pass=26`, `dq_fail=0`)
- `bizdq_glb_20260406T152610` (`pipeline_run=success`, `dq_pass=20`, `dq_fail=0`)
- `bizdq_hist_20260406T152610` (`pipeline_run=success`, `dq_pass=32`, `dq_fail=0`)
- `monitor_coin_history_stability.py --lookback-days 7` (`success_rate=71.43%`, baseline tuning diperbarui)
- `check_ops_alerts.py --lookback-hours 24` (`failed_pipeline_run_count=2`, `failed_dq_check_count=2`; alerting rule tervalidasi)
- `tunehist_20260406T0900` (`pipeline_run=success`, `rows_extracted=100`, `rows_loaded=27740`, `dq_pass=32`, `dq_fail=0`)
- `opsalert_20260406T0910` (`pipeline_run=failed` by design; rule alert tervalidasi)

---

## 7. Next Actions

1. Verifikasi run terjadwal `coin_history_daily_dag` selama 3-7 hari pasca tuning baru untuk memastikan stabilitas meningkat.
2. Tambahkan kanal notifikasi eksternal (Slack/Email/Webhook) untuk `ops_alerting_dag`.
3. Definisikan SLA dan threshold alert yang lebih granular per DAG.
