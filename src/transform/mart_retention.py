from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

from src.db import mysql_connection


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} harus integer.") from exc


def _retention_disabled_result(table_name: str, retention_days: int) -> dict[str, Any]:
    return {
        "table_name": table_name,
        "retention_enabled": False,
        "retention_days": retention_days,
        "deleted_rows": 0,
        "cutoff_utc": None,
    }


def _delete_before_datetime(
    *,
    table_name: str,
    column_name: str,
    cutoff_utc: datetime,
    retention_days: int,
) -> dict[str, Any]:
    if not _env_bool("MART_RETENTION_ENABLED", False):
        return _retention_disabled_result(table_name, retention_days)
    if retention_days <= 0:
        return _retention_disabled_result(table_name, retention_days)

    delete_sql = f"""
    DELETE FROM {table_name}
    WHERE {column_name} < %s
    """
    with mysql_connection(layer="mart") as connection:
        with connection.cursor() as cursor:
            cursor.execute(delete_sql, (cutoff_utc,))
            deleted_rows = int(cursor.rowcount)

    return {
        "table_name": table_name,
        "retention_enabled": True,
        "retention_days": retention_days,
        "deleted_rows": deleted_rows,
        "cutoff_utc": cutoff_utc,
    }


def _delete_before_date(
    *,
    table_name: str,
    column_name: str,
    cutoff_date_utc: date,
    retention_days: int,
) -> dict[str, Any]:
    if not _env_bool("MART_RETENTION_ENABLED", False):
        return _retention_disabled_result(table_name, retention_days)
    if retention_days <= 0:
        return _retention_disabled_result(table_name, retention_days)

    delete_sql = f"""
    DELETE FROM {table_name}
    WHERE {column_name} < %s
    """
    with mysql_connection(layer="mart") as connection:
        with connection.cursor() as cursor:
            cursor.execute(delete_sql, (cutoff_date_utc,))
            deleted_rows = int(cursor.rowcount)

    return {
        "table_name": table_name,
        "retention_enabled": True,
        "retention_days": retention_days,
        "deleted_rows": deleted_rows,
        "cutoff_utc": cutoff_date_utc,
    }


def cleanup_mart_top_coins_latest(*, now_utc: datetime | None = None) -> dict[str, Any]:
    reference_utc = now_utc or _utc_now_naive()
    retention_days = _env_int("MART_TOP_COINS_RETENTION_DAYS", 90)
    cutoff_utc = reference_utc - timedelta(days=retention_days)
    return _delete_before_datetime(
        table_name="mart_top_coins_latest",
        column_name="snapshot_ts_utc",
        cutoff_utc=cutoff_utc,
        retention_days=retention_days,
    )


def cleanup_mart_market_summary_daily(*, now_utc: datetime | None = None) -> dict[str, Any]:
    reference_utc = now_utc or _utc_now_naive()
    retention_days = _env_int("MART_MARKET_SUMMARY_RETENTION_DAYS", 0)
    cutoff_date_utc = (reference_utc - timedelta(days=retention_days)).date()
    return _delete_before_date(
        table_name="mart_market_summary_daily",
        column_name="metric_date",
        cutoff_date_utc=cutoff_date_utc,
        retention_days=retention_days,
    )


def cleanup_mart_coin_daily_metrics(*, now_utc: datetime | None = None) -> dict[str, Any]:
    reference_utc = now_utc or _utc_now_naive()
    retention_days = _env_int("MART_COIN_DAILY_RETENTION_DAYS", 730)
    cutoff_date_utc = (reference_utc - timedelta(days=retention_days)).date()
    return _delete_before_date(
        table_name="mart_coin_daily_metrics",
        column_name="metric_date",
        cutoff_date_utc=cutoff_date_utc,
        retention_days=retention_days,
    )


def cleanup_mart_coin_rolling_metrics(*, now_utc: datetime | None = None) -> dict[str, Any]:
    reference_utc = now_utc or _utc_now_naive()
    retention_days = _env_int("MART_COIN_ROLLING_RETENTION_DAYS", 730)
    cutoff_date_utc = (reference_utc - timedelta(days=retention_days)).date()
    return _delete_before_date(
        table_name="mart_coin_rolling_metrics",
        column_name="metric_date",
        cutoff_date_utc=cutoff_date_utc,
        retention_days=retention_days,
    )
