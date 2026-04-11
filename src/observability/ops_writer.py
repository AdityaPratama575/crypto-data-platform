from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.db import mysql_connection


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    return ts.astimezone(timezone.utc).replace(tzinfo=None)


def upsert_pipeline_run_started(
    *,
    dag_id: str,
    dag_run_id: str,
    logical_run_ts_utc: datetime,
    started_at_utc: datetime,
    message: str | None = None,
) -> None:
    sql = """
    INSERT INTO pipeline_run (
        dag_id,
        dag_run_id,
        logical_run_ts_utc,
        started_at_utc,
        ended_at_utc,
        run_status,
        message
    )
    VALUES (%s, %s, %s, %s, NULL, %s, %s)
    ON DUPLICATE KEY UPDATE
        logical_run_ts_utc = VALUES(logical_run_ts_utc),
        started_at_utc = VALUES(started_at_utc),
        ended_at_utc = NULL,
        run_status = VALUES(run_status),
        message = VALUES(message)
    """
    params = (
        dag_id,
        dag_run_id,
        _naive_utc(logical_run_ts_utc),
        _naive_utc(started_at_utc),
        "running",
        message,
    )

    with mysql_connection(layer="ops") as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)


def mark_pipeline_run_success(
    *,
    dag_id: str,
    dag_run_id: str,
    ended_at_utc: datetime,
    message: str | None = None,
) -> None:
    sql = """
    UPDATE pipeline_run
    SET ended_at_utc = %s,
        run_status = %s,
        message = %s
    WHERE dag_id = %s
      AND dag_run_id = %s
    """
    params = (_naive_utc(ended_at_utc), "success", message, dag_id, dag_run_id)

    with mysql_connection(layer="ops") as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)


def mark_pipeline_run_failed(
    *,
    dag_id: str,
    dag_run_id: str,
    ended_at_utc: datetime,
    error_message: str,
) -> None:
    sql = """
    UPDATE pipeline_run
    SET ended_at_utc = %s,
        run_status = %s,
        message = %s
    WHERE dag_id = %s
      AND dag_run_id = %s
    """
    params = (_naive_utc(ended_at_utc), "failed", error_message, dag_id, dag_run_id)

    with mysql_connection(layer="ops") as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)


def insert_task_run(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    started_at_utc: datetime,
    ended_at_utc: datetime,
    task_status: str,
    rows_extracted: int | None = None,
    rows_loaded: int | None = None,
    rows_merged: int | None = None,
    error_message: str | None = None,
) -> None:
    sql = """
    INSERT INTO task_run (
        dag_id,
        dag_run_id,
        task_id,
        started_at_utc,
        ended_at_utc,
        task_status,
        rows_extracted,
        rows_loaded,
        rows_merged,
        error_message
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        dag_id,
        dag_run_id,
        task_id,
        _naive_utc(started_at_utc),
        _naive_utc(ended_at_utc),
        task_status,
        rows_extracted,
        rows_loaded,
        rows_merged,
        error_message,
    )

    with mysql_connection(layer="ops") as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)


def insert_dq_check_result(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    check_name: str,
    target_table: str,
    check_status: str,
    failed_row_count: int,
    checked_at_utc: datetime,
    detail: dict[str, Any] | None = None,
) -> None:
    sql = """
    INSERT INTO dq_check_result (
        dag_id,
        dag_run_id,
        task_id,
        check_name,
        target_table,
        check_status,
        failed_row_count,
        checked_at_utc,
        detail_json
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    detail_json = json.dumps(detail) if detail is not None else None
    params = (
        dag_id,
        dag_run_id,
        task_id,
        check_name,
        target_table,
        check_status,
        failed_row_count,
        _naive_utc(checked_at_utc),
        detail_json,
    )

    with mysql_connection(layer="ops") as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
