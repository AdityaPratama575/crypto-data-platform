from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from pendulum import datetime


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.extract import extract_coin_markets_top_n  # noqa: E402
from src.load import (  # noqa: E402
    load_coin_markets_snapshot_rows,
    merge_dim_coin_from_raw_coin_markets_batch,
    merge_fact_coin_market_snapshot_from_raw_batch,
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
    run_and_log_core_fact_coin_market_business_checks,
    run_and_log_mart_top_coins_latest_business_checks,
    run_and_log_mart_table_dq_checks,
    run_and_log_raw_batch_dq_checks,
)
from src.transform import (  # noqa: E402
    cleanup_mart_top_coins_latest,
    refresh_mart_top_coins_latest,
)


@dag(
    dag_id="market_snapshot_hourly_dag",
    schedule="5 * * * *",
    start_date=datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "crypto-data-platform", "retries": 1},
    tags=["crypto", "market", "v1"],
)
def market_snapshot_hourly_pipeline():
    @task(task_id="extract_write_load_coin_markets")
    def extract_write_load_coin_markets(**context: Any) -> dict[str, Any]:
        dag_id = "market_snapshot_hourly_dag"
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

        task_run_id = "extract_write_load_coin_markets"
        if task_instance is not None:
            task_run_id = f"{task_instance.task_id}_try_{task_instance.try_number}"

        upsert_pipeline_run_started(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            logical_run_ts_utc=logical_run_ts_utc,
            started_at_utc=started_at_utc,
            message="extract_write_load_coin_markets started",
        )

        task_status = "success"
        error_message: str | None = None
        batch_id: str | None = None
        rows_extracted = 0
        rows_loaded = 0
        rows_merged = 0
        rows_deleted_mart_retention = 0

        try:
            snapshot = extract_coin_markets_top_n(
                dag_run_id=dag_run_id,
                task_run_id=task_run_id,
                logical_run_ts_utc=logical_run_ts_utc,
            )
            batch_id = snapshot["batch_id"]

            raw_file = write_raw_payload(
                endpoint_name="coins_markets",
                batch_id=snapshot["batch_id"],
                source_endpoint=snapshot["source_endpoint"],
                request_params=snapshot["request_params"],
                payload=snapshot["payload"],
                logical_run_ts_utc=snapshot["logical_run_ts_utc"],
                extracted_at_utc=snapshot["extracted_at_utc"],
            )

            rows_extracted = len(snapshot["payload"])
            rows_loaded = load_coin_markets_snapshot_rows(snapshot)
            raw_dq_results = run_and_log_raw_batch_dq_checks(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
                target_table="raw_coin_markets_snapshot",
                batch_id=batch_id,
                required_not_null_columns=[
                    "batch_id",
                    "coin_id",
                    "snapshot_ts_utc",
                    "raw_payload_json",
                ],
                unique_key_columns=["coin_id", "vs_currency", "page_no"],
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

            rows_merged_dim = merge_dim_coin_from_raw_coin_markets_batch(batch_id)
            rows_merged = merge_fact_coin_market_snapshot_from_raw_batch(batch_id)
            core_dq_results = run_and_log_core_batch_dq_checks(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
                target_table="fact_coin_market_snapshot",
                source_batch_id=batch_id,
                required_not_null_columns=[
                    "snapshot_ts_utc",
                    "coin_id",
                    "vs_currency",
                    "source_batch_id",
                ],
                unique_key_columns=["snapshot_ts_utc", "coin_id", "vs_currency"],
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

            core_business_dq_results = run_and_log_core_fact_coin_market_business_checks(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
                source_batch_id=batch_id,
            )
            core_business_failed_checks = sum(
                1
                for result in core_business_dq_results
                if result["check_status"] != "pass"
            )
            if core_business_failed_checks > 0:
                task_status = "failed"
                error_message = (
                    "DQ business core gagal untuk fact_coin_market_snapshot: "
                    f"{core_business_failed_checks} check(s) failed."
                )
                raise RuntimeError(error_message)

            rows_refreshed_mart = refresh_mart_top_coins_latest()
            mart_dq_results = run_and_log_mart_table_dq_checks(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
                target_table="mart_top_coins_latest",
                required_not_null_columns=[
                    "snapshot_ts_utc",
                    "coin_id",
                    "coin_name",
                    "vs_currency",
                ],
                unique_key_columns=["snapshot_ts_utc", "coin_id", "vs_currency"],
            )
            mart_failed_checks = sum(
                1 for result in mart_dq_results if result["check_status"] != "pass"
            )
            if mart_failed_checks > 0:
                task_status = "failed"
                error_message = (
                    f"DQ mart gagal setelah refresh: {mart_failed_checks} check(s) failed."
                )
                raise RuntimeError(error_message)

            mart_business_dq_results = run_and_log_mart_top_coins_latest_business_checks(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_run_id,
            )
            mart_business_failed_checks = sum(
                1
                for result in mart_business_dq_results
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
                    "DQ business mart gagal untuk mart_top_coins_latest: "
                    f"{mart_business_failed_checks} check(s) failed."
                )
                raise RuntimeError(error_message)

            retention_result = cleanup_mart_top_coins_latest()
            rows_deleted_mart_retention = int(retention_result["deleted_rows"])

            summary = {
                "batch_id": batch_id,
                "raw_file": str(raw_file),
                "payload_count": rows_extracted,
                "inserted_rows": rows_loaded,
                "rows_merged": rows_merged,
                "rows_merged_dim": rows_merged_dim,
                "rows_refreshed_mart": rows_refreshed_mart,
                "rows_deleted_mart_retention": rows_deleted_mart_retention,
                "mart_retention": retention_result,
                "dq_failed_checks": total_failed_checks,
            }
            print(f"[INFO] market_snapshot_hourly summary: {summary}")
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

    extract_write_load_coin_markets()


market_snapshot_hourly_dag = market_snapshot_hourly_pipeline()
