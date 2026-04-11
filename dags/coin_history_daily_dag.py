from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from pendulum import datetime


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.extract import extract_coin_history_top_n  # noqa: E402
from src.load import (  # noqa: E402
    load_coin_market_chart_point_rows,
    merge_fact_coin_price_history_from_raw_batch,
    write_raw_payload,
)
from src.observability import (  # noqa: E402
    insert_task_run,
    mark_pipeline_run_failed,
    mark_pipeline_run_success,
    upsert_pipeline_run_started,
    utc_now,
)
from src.quality import (  # noqa: E402
    run_and_log_core_batch_dq_checks,
    run_and_log_core_fact_coin_price_history_business_checks,
    run_and_log_mart_coin_daily_metrics_business_checks,
    run_and_log_mart_coin_rolling_metrics_business_checks,
    run_and_log_mart_table_dq_checks,
    run_and_log_raw_batch_dq_checks,
)
from src.transform import (  # noqa: E402
    cleanup_mart_coin_daily_metrics,
    cleanup_mart_coin_rolling_metrics,
    refresh_mart_coin_daily_metrics,
    refresh_mart_coin_rolling_metrics,
)


@dag(
    dag_id="coin_history_daily_dag",
    schedule="15 3 * * *",
    start_date=datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "crypto-data-platform", "retries": 1},
    tags=["crypto", "history", "v1"],
)
def coin_history_daily_pipeline():
    @task(task_id="extract_write_load_coin_history")
    def extract_write_load_coin_history(**context: Any) -> dict[str, Any]:
        dag_id = "coin_history_daily_dag"
        dag_run = context.get("dag_run")
        logical_date = context.get("logical_date")
        task_instance = context.get("task_instance")
        started_at_utc = utc_now()
        dag_run_id = (
            dag_run.run_id
            if dag_run is not None and dag_run.run_id
            else f"manual_{started_at_utc.strftime('%Y%m%dT%H%M%S')}"
        )
        logical_run_ts_utc = logical_date or started_at_utc

        task_run_id = "extract_write_load_coin_history"
        if task_instance is not None:
            task_run_id = f"{task_instance.task_id}_try_{task_instance.try_number}"

        upsert_pipeline_run_started(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            logical_run_ts_utc=logical_run_ts_utc,
            started_at_utc=started_at_utc,
            message="extract_write_load_coin_history started",
        )

        task_status = "success"
        error_message: str | None = None
        batch_id: str | None = None
        rows_extracted = 0
        rows_loaded = 0
        rows_merged = 0
        rows_deleted_mart_retention = 0

        run_conf: dict[str, Any] = {}
        if dag_run and isinstance(dag_run.conf, dict):
            run_conf = dag_run.conf

        top_n_raw = run_conf.get("top_n")
        days_raw = run_conf.get("days", 1)
        batch_size_raw = run_conf.get("batch_size")
        request_pause_raw = run_conf.get("request_pause_seconds")
        batch_pause_raw = run_conf.get("batch_pause_seconds")
        client_max_retries_raw = run_conf.get("client_max_retries")
        client_backoff_seconds_raw = run_conf.get("client_backoff_seconds")

        top_n: int | None = None
        if top_n_raw is not None:
            top_n = int(top_n_raw)
        days = int(days_raw)

        batch_size: int | None = None
        if batch_size_raw is not None:
            batch_size = int(batch_size_raw)

        request_pause_seconds: float | None = None
        if request_pause_raw is not None:
            request_pause_seconds = float(request_pause_raw)

        batch_pause_seconds: float | None = None
        if batch_pause_raw is not None:
            batch_pause_seconds = float(batch_pause_raw)

        client_max_retries: int | None = None
        if client_max_retries_raw is not None:
            client_max_retries = int(client_max_retries_raw)

        client_backoff_seconds: int | None = None
        if client_backoff_seconds_raw is not None:
            client_backoff_seconds = int(client_backoff_seconds_raw)

        try:
            snapshot = extract_coin_history_top_n(
                top_n=top_n,
                days=days,
                batch_size=batch_size,
                request_pause_seconds=request_pause_seconds,
                batch_pause_seconds=batch_pause_seconds,
                client_max_retries=client_max_retries,
                client_backoff_seconds=client_backoff_seconds,
                dag_run_id=dag_run_id,
                task_run_id=task_run_id,
                logical_run_ts_utc=logical_run_ts_utc,
            )
            batch_id = snapshot["batch_id"]

            raw_file_count = 0
            for coin_payload in snapshot["coin_payloads"]:
                coin_id = coin_payload["coin_id"]
                write_raw_payload(
                    endpoint_name="coin_market_chart",
                    batch_id=snapshot["batch_id"],
                    source_endpoint=snapshot["source_endpoint"],
                    request_params=coin_payload["request_params"],
                    payload=coin_payload["payload"],
                    logical_run_ts_utc=snapshot["logical_run_ts_utc"],
                    extracted_at_utc=snapshot["extracted_at_utc"],
                    file_suffix=coin_id,
                )
                raw_file_count += 1

            rows_extracted = len(snapshot["coin_payloads"])
            rows_loaded = load_coin_market_chart_point_rows(snapshot)
            raw_dq_results = run_and_log_raw_batch_dq_checks(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
                target_table="raw_coin_market_chart_point",
                batch_id=batch_id,
                required_not_null_columns=[
                    "batch_id",
                    "coin_id",
                    "price_ts_utc",
                    "raw_payload_json",
                ],
                unique_key_columns=["coin_id", "vs_currency", "price_ts_utc"],
            )
            raw_failed_checks = sum(
                1 for result in raw_dq_results if result["check_status"] != "pass"
            )
            if raw_failed_checks > 0:
                task_status = "failed"
                error_message = (
                    f"DQ raw gagal untuk batch {batch_id}: {raw_failed_checks} check(s) failed."
                )
                raise RuntimeError(error_message)

            rows_merged = merge_fact_coin_price_history_from_raw_batch(batch_id)
            core_dq_results = run_and_log_core_batch_dq_checks(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
                target_table="fact_coin_price_history",
                source_batch_id=batch_id,
                required_not_null_columns=[
                    "price_ts_utc",
                    "coin_id",
                    "vs_currency",
                    "source_batch_id",
                ],
                unique_key_columns=["price_ts_utc", "coin_id", "vs_currency"],
            )
            core_failed_checks = sum(
                1 for result in core_dq_results if result["check_status"] != "pass"
            )
            if core_failed_checks > 0:
                task_status = "failed"
                error_message = (
                    f"DQ core gagal untuk batch {batch_id}: {core_failed_checks} check(s) failed."
                )
                raise RuntimeError(error_message)

            core_business_dq_results = (
                run_and_log_core_fact_coin_price_history_business_checks(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    task_id=task_run_id,
                    source_batch_id=batch_id,
                )
            )
            core_business_failed_checks = sum(
                1
                for result in core_business_dq_results
                if result["check_status"] != "pass"
            )
            if core_business_failed_checks > 0:
                task_status = "failed"
                error_message = (
                    "DQ business core gagal untuk fact_coin_price_history: "
                    f"{core_business_failed_checks} check(s) failed."
                )
                raise RuntimeError(error_message)

            rows_refreshed_mart_daily = refresh_mart_coin_daily_metrics()
            rows_refreshed_mart_rolling = refresh_mart_coin_rolling_metrics()

            mart_daily_dq_results = run_and_log_mart_table_dq_checks(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
                target_table="mart_coin_daily_metrics",
                required_not_null_columns=[
                    "metric_date",
                    "coin_id",
                    "coin_name",
                    "vs_currency",
                ],
                unique_key_columns=["metric_date", "coin_id", "vs_currency"],
            )
            mart_rolling_dq_results = run_and_log_mart_table_dq_checks(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
                target_table="mart_coin_rolling_metrics",
                required_not_null_columns=[
                    "metric_date",
                    "coin_id",
                    "coin_name",
                    "vs_currency",
                ],
                unique_key_columns=["metric_date", "coin_id", "vs_currency"],
            )
            mart_failed_checks = sum(
                1
                for result in mart_daily_dq_results + mart_rolling_dq_results
                if result["check_status"] != "pass"
            )
            if mart_failed_checks > 0:
                task_status = "failed"
                error_message = (
                    "DQ mart structural gagal setelah refresh history marts: "
                    f"{mart_failed_checks} check(s) failed."
                )
                raise RuntimeError(error_message)

            mart_daily_business_dq_results = (
                run_and_log_mart_coin_daily_metrics_business_checks(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    task_id=task_run_id,
                )
            )
            mart_rolling_business_dq_results = (
                run_and_log_mart_coin_rolling_metrics_business_checks(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    task_id=task_run_id,
                )
            )
            mart_business_failed_checks = sum(
                1
                for result in mart_daily_business_dq_results
                + mart_rolling_business_dq_results
                if result["check_status"] != "pass"
            )
            total_failed_checks = (
                raw_failed_checks
                + core_failed_checks
                + core_business_failed_checks
                + mart_failed_checks
                + mart_business_failed_checks
            )
            if total_failed_checks > 0:
                task_status = "failed"
                error_message = (
                    "DQ business mart gagal setelah refresh history marts: "
                    f"{mart_business_failed_checks} check(s) failed."
                )
                raise RuntimeError(error_message)

            daily_retention_result = cleanup_mart_coin_daily_metrics()
            rolling_retention_result = cleanup_mart_coin_rolling_metrics()
            rows_deleted_mart_retention = int(daily_retention_result["deleted_rows"]) + int(
                rolling_retention_result["deleted_rows"]
            )

            summary = {
                "batch_id": batch_id,
                "raw_file_pattern": "data/raw/coin_market_chart/*",
                "raw_file_count": raw_file_count,
                "payload_count": rows_extracted,
                "inserted_rows": rows_loaded,
                "rows_merged": rows_merged,
                "rows_refreshed_mart_daily": rows_refreshed_mart_daily,
                "rows_refreshed_mart_rolling": rows_refreshed_mart_rolling,
                "rows_deleted_mart_retention": rows_deleted_mart_retention,
                "mart_retention": {
                    "mart_coin_daily_metrics": daily_retention_result,
                    "mart_coin_rolling_metrics": rolling_retention_result,
                },
                "history_runtime": {
                    "batch_size": snapshot.get("batch_size"),
                    "request_pause_seconds": snapshot.get("request_pause_seconds"),
                    "batch_pause_seconds": snapshot.get("batch_pause_seconds"),
                    "client_max_retries": snapshot.get("client_max_retries"),
                    "client_backoff_seconds": snapshot.get("client_backoff_seconds"),
                },
                "dq_failed_checks": total_failed_checks,
            }
            print(f"[INFO] coin_history_daily summary: {summary}")
            return summary
        except Exception as exc:
            task_status = "failed"
            if error_message is None:
                error_message = str(exc)
            raise
        finally:
            ended_at_utc = utc_now()
            insert_task_run(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
                started_at_utc=started_at_utc,
                ended_at_utc=ended_at_utc,
                task_status=task_status,
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                rows_merged=rows_merged,
                error_message=error_message,
            )
            if task_status == "success":
                mark_pipeline_run_success(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    ended_at_utc=ended_at_utc,
                    message=(
                        f"batch_id={batch_id}; rows_extracted={rows_extracted}; "
                        f"rows_loaded={rows_loaded}; rows_merged={rows_merged}"
                    ),
                )
            else:
                mark_pipeline_run_failed(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    ended_at_utc=ended_at_utc,
                    error_message=error_message or "Task failed without error message.",
                )

    extract_write_load_coin_history()


coin_history_daily_dag = coin_history_daily_pipeline()
