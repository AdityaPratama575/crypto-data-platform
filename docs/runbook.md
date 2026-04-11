# Runbook Operasional - Crypto Data Platform

Dokumen ini adalah panduan operasional harian untuk backfill, rerun, failure handling, retention, dan alerting.

Terakhir diperbarui: 2026-04-06

---

## 1. Scope

Runbook ini mencakup DAG v1:
- `reference_coin_list_dag`
- `market_snapshot_hourly_dag`
- `global_market_hourly_dag`
- `coin_history_daily_dag`
- `ops_alerting_dag`

---

## 2. Checklist Harian

1. Pastikan container Airflow up:
   - `make ps`
2. Cek import DAG:
   - `docker exec crypto_airflow_scheduler airflow dags list-import-errors`
3. Cek alert operasional:
   - `.venv/bin/python scripts/check_ops_alerts.py --lookback-hours 6 --exclude-dag-id ops_alerting_dag`
4. Cek stabilitas history:
   - `.venv/bin/python scripts/monitor_coin_history_stability.py --lookback-days 7`

---

## 3. Backfill Playbook

Catatan:
- Gunakan run ID unik (`backfill_<dag>_<timestamp>`).
- Jangan jalankan banyak backfill paralel untuk `coin_history_daily_dag` tanpa throttle.
- Alternatif lebih sederhana: pakai wrapper `scripts/run_backfill.py`.

Contoh wrapper:

```bash
.venv/bin/python scripts/run_backfill.py --dag-id reference_coin_list_dag
.venv/bin/python scripts/run_backfill.py --dag-id coin_history_daily_dag --conf '{"top_n":100,"days":1,"batch_size":10,"request_pause_seconds":1.5,"batch_pause_seconds":8.0,"client_max_retries":6,"client_backoff_seconds":4}'
```

### 3.1 Reference coin list

```bash
docker exec crypto_airflow_scheduler \
  airflow dags trigger reference_coin_list_dag \
  --run-id backfill_ref_20260406T000000
```

### 3.2 Market snapshot hourly

```bash
docker exec crypto_airflow_scheduler \
  airflow dags trigger market_snapshot_hourly_dag \
  --run-id backfill_mkt_20260406T000000
```

### 3.3 Global market hourly

```bash
docker exec crypto_airflow_scheduler \
  airflow dags trigger global_market_hourly_dag \
  --run-id backfill_glb_20260406T000000
```

### 3.4 Coin history daily (top 100)

```bash
docker exec crypto_airflow_scheduler \
  airflow dags trigger coin_history_daily_dag \
  --run-id backfill_hist_20260406T000000 \
  --conf '{"top_n":100,"days":1,"batch_size":10,"request_pause_seconds":1.5,"batch_pause_seconds":8.0,"client_max_retries":6,"client_backoff_seconds":4}'
```

---

## 4. Rerun Playbook

Prinsip:
- DAG idempotent; gunakan run ID baru, jangan overwrite run ID lama.
- Untuk issue transient (network/429), lakukan rerun dengan conf throttle lebih konservatif.

Contoh rerun `coin_history_daily_dag`:

```bash
docker exec crypto_airflow_scheduler \
  airflow dags trigger coin_history_daily_dag \
  --run-id rerun_hist_20260406T010000 \
  --conf '{"top_n":100,"days":1,"batch_size":8,"request_pause_seconds":2.0,"batch_pause_seconds":10.0,"client_max_retries":6,"client_backoff_seconds":5}'
```

---

## 5. Failure Handling per DAG

### 5.1 `reference_coin_list_dag`
- Gejala umum: koneksi CoinGecko/MySQL gagal.
- Tindakan:
  1. Cek `task_run.error_message`.
  2. Verifikasi `.env` (`COINGECKO_API_KEY`, `MYSQL_*`).
  3. Trigger ulang.

### 5.2 `market_snapshot_hourly_dag` / `global_market_hourly_dag`
- Gejala umum: API transient error atau DQ fail.
- Tindakan:
  1. Cek `crypto_ops.dq_check_result` untuk `check_status='fail'`.
  2. Cek apakah issue terjadi pada satu batch atau berulang.
  3. Rerun manual satu kali setelah validasi.

### 5.3 `coin_history_daily_dag`
- Gejala umum:
  - `429` rate-limit CoinGecko
  - DQ raw duplicate
- Tindakan:
  1. Cek kategori error dari:
     - `.venv/bin/python scripts/monitor_coin_history_stability.py --lookback-days 7`
  2. Jika dominan `429`: naikkan pause/retry (`COIN_HISTORY_*`).
  3. Jika dominan duplicate raw:
     - pastikan versi terbaru `src/load/load_raw_tables.py` sudah deploy (fix presisi ms pada `price_ts_utc`, April 6, 2026)
     - rerun satu batch validasi top 100.

---

## 6. Retention / Cleanup Policy (Mart)

Retention dijalankan setelah refresh mart pada DAG terkait, dengan env-driven policy.

Master switch:
- `MART_RETENTION_ENABLED` (`true`/`false`)

Policy default:
- `MART_TOP_COINS_RETENTION_DAYS=90`
- `MART_MARKET_SUMMARY_RETENTION_DAYS=0` (`0` berarti tidak dihapus)
- `MART_COIN_DAILY_RETENTION_DAYS=730`
- `MART_COIN_ROLLING_RETENTION_DAYS=730`

Verifikasi cepat:

```sql
SELECT MIN(snapshot_ts_utc), MAX(snapshot_ts_utc), COUNT(*) FROM crypto_mart.mart_top_coins_latest;
SELECT MIN(metric_date), MAX(metric_date), COUNT(*) FROM crypto_mart.mart_coin_daily_metrics;
SELECT MIN(metric_date), MAX(metric_date), COUNT(*) FROM crypto_mart.mart_coin_rolling_metrics;
```

---

## 7. Alerting Otomatis dari `crypto_ops`

### 7.1 DAG alerting
- DAG: `ops_alerting_dag`
- Schedule: setiap 15 menit (`*/15 * * * *`)
- Rule:
  - alert jika ada `pipeline_run.run_status='failed'`
  - alert jika ada DQ fail (`check_status <> 'pass'` atau `failed_row_count > 0`)

### 7.2 Konfigurasi alert
- `OPS_ALERT_LOOKBACK_HOURS=6`
- `OPS_ALERT_MAX_ITEMS=20`
- `OPS_ALERT_EXCLUDE_DAG_IDS=ops_alerting_dag`

### 7.3 Manual check

```bash
.venv/bin/python scripts/check_ops_alerts.py --lookback-hours 24 --exclude-dag-id ops_alerting_dag
```

---

## 8. Verifikasi Pasca Perbaikan

1. Trigger DAG terkait.
2. Pastikan status run `success`.
3. Validasi `task_run`:

```sql
SELECT dag_id, dag_run_id, task_status, rows_extracted, rows_loaded, rows_merged
FROM crypto_ops.task_run
ORDER BY task_run_id DESC
LIMIT 20;
```

4. Validasi DQ:

```sql
SELECT dag_id, dag_run_id, target_table, check_name, check_status, failed_row_count
FROM crypto_ops.dq_check_result
ORDER BY dq_result_id DESC
LIMIT 50;
```

5. Update `docs/pipeline_flow.md` dengan hasil run terbaru.
