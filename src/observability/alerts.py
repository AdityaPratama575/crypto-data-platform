from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from src.db import mysql_connection


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} harus integer.") from exc


def _env_csv(name: str) -> list[str]:
    raw_value = os.getenv(name, "")
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _build_exclude_clause(*, column_name: str, values: list[str]) -> tuple[str, tuple[Any, ...]]:
    if not values:
        return "", ()
    placeholders = ", ".join(["%s"] * len(values))
    return f" AND {column_name} NOT IN ({placeholders})", tuple(values)


def evaluate_ops_alerts(
    *,
    lookback_hours: int | None = None,
    max_items: int | None = None,
    excluded_dag_ids: list[str] | None = None,
) -> dict[str, Any]:
    selected_lookback_hours = (
        lookback_hours if lookback_hours is not None else _env_int("OPS_ALERT_LOOKBACK_HOURS", 6)
    )
    if selected_lookback_hours <= 0:
        raise ValueError("lookback_hours harus lebih besar dari 0.")

    selected_max_items = max_items if max_items is not None else _env_int("OPS_ALERT_MAX_ITEMS", 20)
    if selected_max_items <= 0:
        raise ValueError("max_items harus lebih besar dari 0.")

    selected_excluded_dag_ids = (
        excluded_dag_ids
        if excluded_dag_ids is not None
        else _env_csv("OPS_ALERT_EXCLUDE_DAG_IDS")
    )

    window_start_utc = _utc_now_naive() - timedelta(hours=selected_lookback_hours)
    exclude_pipeline_sql, exclude_pipeline_params = _build_exclude_clause(
        column_name="dag_id",
        values=selected_excluded_dag_ids,
    )

    failed_pipeline_sql = f"""
    SELECT
        dag_id,
        dag_run_id,
        logical_run_ts_utc,
        started_at_utc,
        ended_at_utc,
        run_status,
        message
    FROM pipeline_run
    WHERE started_at_utc >= %s
      AND run_status = 'failed'
      {exclude_pipeline_sql}
    ORDER BY started_at_utc DESC
    LIMIT %s
    """
    failed_pipeline_params: tuple[Any, ...] = (
        window_start_utc,
        *exclude_pipeline_params,
        selected_max_items,
    )

    exclude_dq_sql, exclude_dq_params = _build_exclude_clause(
        column_name="dag_id",
        values=selected_excluded_dag_ids,
    )
    failed_dq_sql = f"""
    SELECT
        dag_id,
        dag_run_id,
        task_id,
        target_table,
        check_name,
        check_status,
        failed_row_count,
        checked_at_utc
    FROM dq_check_result
    WHERE checked_at_utc >= %s
      AND (check_status <> 'pass' OR COALESCE(failed_row_count, 0) > 0)
      {exclude_dq_sql}
    ORDER BY checked_at_utc DESC
    LIMIT %s
    """
    failed_dq_params: tuple[Any, ...] = (
        window_start_utc,
        *exclude_dq_params,
        selected_max_items,
    )

    with mysql_connection(layer="ops") as connection:
        with connection.cursor() as cursor:
            cursor.execute(failed_pipeline_sql, failed_pipeline_params)
            failed_pipeline_runs = cursor.fetchall()

            cursor.execute(failed_dq_sql, failed_dq_params)
            failed_dq_checks = cursor.fetchall()

    failed_pipeline_run_count = len(failed_pipeline_runs)
    failed_dq_check_count = len(failed_dq_checks)

    return {
        "window_start_utc": window_start_utc,
        "lookback_hours": selected_lookback_hours,
        "max_items": selected_max_items,
        "excluded_dag_ids": selected_excluded_dag_ids,
        "failed_pipeline_run_count": failed_pipeline_run_count,
        "failed_dq_check_count": failed_dq_check_count,
        "failed_pipeline_runs": failed_pipeline_runs,
        "failed_dq_checks": failed_dq_checks,
        "has_alert": (failed_pipeline_run_count + failed_dq_check_count) > 0,
    }


def assert_no_ops_alerts(
    *,
    lookback_hours: int | None = None,
    max_items: int | None = None,
    excluded_dag_ids: list[str] | None = None,
) -> dict[str, Any]:
    report = evaluate_ops_alerts(
        lookback_hours=lookback_hours,
        max_items=max_items,
        excluded_dag_ids=excluded_dag_ids,
    )
    if report["has_alert"]:
        raise RuntimeError(
            "OPS alert terdeteksi: "
            f"failed_pipeline_run_count={report['failed_pipeline_run_count']}, "
            f"failed_dq_check_count={report['failed_dq_check_count']}, "
            f"window_start_utc={report['window_start_utc']}"
        )
    return report
