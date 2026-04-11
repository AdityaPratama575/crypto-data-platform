from __future__ import annotations

import os
import re
from typing import Any

from src.db import mysql_connection
from src.observability import insert_dq_check_result, utc_now


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> None:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Identifier SQL tidak valid: {name}")


def _validate_identifiers(names: list[str]) -> None:
    for name in names:
        _validate_identifier(name)


def _validate_layer(layer: str) -> str:
    layer_name = layer.lower().strip()
    if layer_name not in {"raw", "core", "mart"}:
        raise ValueError("layer harus salah satu dari: raw, core, mart")
    return layer_name


def _make_result(
    *,
    check_name: str,
    target_table: str,
    failed_row_count: int,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    status = "pass" if failed_row_count == 0 else "fail"
    return {
        "check_name": check_name,
        "target_table": target_table,
        "check_status": status,
        "failed_row_count": failed_row_count,
        "detail": detail or {},
    }


def _extract_failed_row_count(row: Any) -> int:
    if row is None:
        return 0

    if isinstance(row, dict):
        if "failed_row_count" in row:
            return int(row["failed_row_count"] or 0)
        if "row_count" in row:
            return int(row["row_count"] or 0)

    if isinstance(row, (tuple, list)) and row:
        return int(row[0] or 0)

    return int(row)


def _build_filter_where_clause(
    *,
    filter_column: str | None,
    filter_value: Any | None,
) -> tuple[str, tuple[Any, ...], dict[str, Any]]:
    if filter_column is None:
        return "", (), {}

    _validate_identifier(filter_column)
    return (
        f"WHERE {filter_column} = %s",
        (filter_value,),
        {"filter_column": filter_column, "filter_value": filter_value},
    )


def run_and_log_layer_table_dq_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    layer: str,
    target_table: str,
    required_not_null_columns: list[str],
    unique_key_columns: list[str] | None = None,
    filter_column: str | None = None,
    filter_value: Any | None = None,
) -> list[dict[str, Any]]:
    layer_name = _validate_layer(layer)

    _validate_identifier(target_table)
    _validate_identifiers(required_not_null_columns)
    unique_columns = unique_key_columns or []
    if unique_columns:
        _validate_identifiers(unique_columns)

    where_sql, where_params, filter_detail = _build_filter_where_clause(
        filter_column=filter_column,
        filter_value=filter_value,
    )

    results: list[dict[str, Any]] = []

    with mysql_connection(layer=layer_name) as connection:
        with connection.cursor() as cursor:
            count_sql = f"""
            SELECT COUNT(*) AS row_count
            FROM {target_table}
            {where_sql}
            """
            cursor.execute(count_sql, where_params)
            row_count = int(cursor.fetchone()["row_count"])
            count_detail = {"row_count": row_count, "layer": layer_name}
            count_detail.update(filter_detail)

            results.append(
                _make_result(
                    check_name="row_count_positive",
                    target_table=target_table,
                    failed_row_count=0 if row_count > 0 else 1,
                    detail=count_detail,
                )
            )

            for column_name in required_not_null_columns:
                null_sql = f"""
                SELECT COUNT(*) AS failed_row_count
                FROM {target_table}
                {where_sql}
                {"AND" if where_sql else "WHERE"} {column_name} IS NULL
                """
                cursor.execute(null_sql, where_params)
                failed_row_count = int(cursor.fetchone()["failed_row_count"])
                null_detail = {"column": column_name, "layer": layer_name}
                null_detail.update(filter_detail)

                results.append(
                    _make_result(
                        check_name=f"not_null_{column_name}",
                        target_table=target_table,
                        failed_row_count=failed_row_count,
                        detail=null_detail,
                    )
                )

            if unique_columns:
                key_expr = ", ".join(unique_columns)
                dup_sql = f"""
                SELECT COALESCE(SUM(dup.cnt - 1), 0) AS failed_row_count
                FROM (
                    SELECT {key_expr}, COUNT(*) AS cnt
                    FROM {target_table}
                    {where_sql}
                    GROUP BY {key_expr}
                    HAVING COUNT(*) > 1
                ) AS dup
                """
                cursor.execute(dup_sql, where_params)
                duplicate_rows = int(cursor.fetchone()["failed_row_count"])
                duplicate_detail = {"columns": unique_columns, "layer": layer_name}
                duplicate_detail.update(filter_detail)
                results.append(
                    _make_result(
                        check_name=f"duplicate_key_{'_'.join(unique_columns)}",
                        target_table=target_table,
                        failed_row_count=duplicate_rows,
                        detail=duplicate_detail,
                    )
                )

    checked_at_utc = utc_now()
    for result in results:
        insert_dq_check_result(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            check_name=result["check_name"],
            target_table=result["target_table"],
            check_status=result["check_status"],
            failed_row_count=result["failed_row_count"],
            checked_at_utc=checked_at_utc,
            detail=result["detail"],
        )

    return results


def run_and_log_layer_business_dq_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    layer: str,
    target_table: str,
    checks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    layer_name = _validate_layer(layer)
    _validate_identifier(target_table)

    results: list[dict[str, Any]] = []
    with mysql_connection(layer=layer_name) as connection:
        with connection.cursor() as cursor:
            for check in checks:
                check_name = str(check["check_name"])
                sql = str(check["sql"])
                params = tuple(check.get("params") or ())
                detail = dict(check.get("detail") or {})
                detail["layer"] = layer_name

                cursor.execute(sql, params)
                failed_row_count = _extract_failed_row_count(cursor.fetchone())

                results.append(
                    _make_result(
                        check_name=check_name,
                        target_table=target_table,
                        failed_row_count=failed_row_count,
                        detail=detail,
                    )
                )

    checked_at_utc = utc_now()
    for result in results:
        insert_dq_check_result(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            check_name=result["check_name"],
            target_table=result["target_table"],
            check_status=result["check_status"],
            failed_row_count=result["failed_row_count"],
            checked_at_utc=checked_at_utc,
            detail=result["detail"],
        )

    return results


def run_and_log_raw_batch_dq_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    target_table: str,
    batch_id: str,
    required_not_null_columns: list[str],
    unique_key_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    return run_and_log_layer_table_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="raw",
        target_table=target_table,
        required_not_null_columns=required_not_null_columns,
        unique_key_columns=unique_key_columns,
        filter_column="batch_id",
        filter_value=batch_id,
    )


def run_and_log_core_batch_dq_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    target_table: str,
    source_batch_id: str,
    required_not_null_columns: list[str],
    unique_key_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    return run_and_log_layer_table_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="core",
        target_table=target_table,
        required_not_null_columns=required_not_null_columns,
        unique_key_columns=unique_key_columns,
        filter_column="source_batch_id",
        filter_value=source_batch_id,
    )


def run_and_log_mart_table_dq_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    target_table: str,
    required_not_null_columns: list[str],
    unique_key_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    return run_and_log_layer_table_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="mart",
        target_table=target_table,
        required_not_null_columns=required_not_null_columns,
        unique_key_columns=unique_key_columns,
    )


def run_and_log_core_dim_coin_business_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
) -> list[dict[str, Any]]:
    checks = [
        {
            "check_name": "first_seen_lte_last_seen",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM dim_coin
                WHERE first_seen_at_utc > last_seen_at_utc
            """,
        },
        {
            "check_name": "is_active_boolean_flag",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM dim_coin
                WHERE is_active NOT IN (0, 1)
                   OR is_active IS NULL
            """,
        },
        {
            "check_name": "coin_name_not_blank",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM dim_coin
                WHERE TRIM(coin_name) = ''
            """,
        },
    ]
    return run_and_log_layer_business_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="core",
        target_table="dim_coin",
        checks=checks,
    )


def run_and_log_core_fact_coin_market_business_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    source_batch_id: str,
) -> list[dict[str, Any]]:
    expected_vs_currency = os.getenv("COINGECKO_VS_CURRENCY", "usd")
    checks = [
        {
            "check_name": "current_price_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_coin_market_snapshot
                WHERE source_batch_id = %s
                  AND current_price < 0
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "market_cap_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_coin_market_snapshot
                WHERE source_batch_id = %s
                  AND market_cap < 0
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "total_volume_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_coin_market_snapshot
                WHERE source_batch_id = %s
                  AND total_volume < 0
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "market_cap_rank_positive",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_coin_market_snapshot
                WHERE source_batch_id = %s
                  AND (market_cap_rank IS NULL OR market_cap_rank <= 0)
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "vs_currency_matches_config",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_coin_market_snapshot
                WHERE source_batch_id = %s
                  AND vs_currency <> %s
            """,
            "params": (source_batch_id, expected_vs_currency),
            "detail": {
                "source_batch_id": source_batch_id,
                "expected_vs_currency": expected_vs_currency,
            },
        },
    ]
    return run_and_log_layer_business_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="core",
        target_table="fact_coin_market_snapshot",
        checks=checks,
    )


def run_and_log_core_fact_global_market_business_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    source_batch_id: str,
) -> list[dict[str, Any]]:
    checks = [
        {
            "check_name": "total_market_cap_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_global_market
                WHERE source_batch_id = %s
                  AND total_market_cap_usd < 0
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "total_volume_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_global_market
                WHERE source_batch_id = %s
                  AND total_volume_usd < 0
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "market_cap_pct_btc_in_range",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_global_market
                WHERE source_batch_id = %s
                  AND market_cap_percentage_btc IS NOT NULL
                  AND (market_cap_percentage_btc < 0 OR market_cap_percentage_btc > 100)
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "market_cap_pct_eth_in_range",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_global_market
                WHERE source_batch_id = %s
                  AND market_cap_percentage_eth IS NOT NULL
                  AND (market_cap_percentage_eth < 0 OR market_cap_percentage_eth > 100)
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
    ]
    return run_and_log_layer_business_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="core",
        target_table="fact_global_market",
        checks=checks,
    )


def run_and_log_core_fact_coin_price_history_business_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    source_batch_id: str,
) -> list[dict[str, Any]]:
    expected_vs_currency = os.getenv("COINGECKO_VS_CURRENCY", "usd")
    checks = [
        {
            "check_name": "price_value_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_coin_price_history
                WHERE source_batch_id = %s
                  AND price_value < 0
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "market_cap_value_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_coin_price_history
                WHERE source_batch_id = %s
                  AND market_cap_value < 0
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "total_volume_value_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_coin_price_history
                WHERE source_batch_id = %s
                  AND total_volume_value < 0
            """,
            "params": (source_batch_id,),
            "detail": {"source_batch_id": source_batch_id},
        },
        {
            "check_name": "vs_currency_matches_config",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM fact_coin_price_history
                WHERE source_batch_id = %s
                  AND vs_currency <> %s
            """,
            "params": (source_batch_id, expected_vs_currency),
            "detail": {
                "source_batch_id": source_batch_id,
                "expected_vs_currency": expected_vs_currency,
            },
        },
    ]
    return run_and_log_layer_business_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="core",
        target_table="fact_coin_price_history",
        checks=checks,
    )


def run_and_log_mart_top_coins_latest_business_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
) -> list[dict[str, Any]]:
    expected_top_n = int(os.getenv("TRACKED_COINS_TOP_N", "100"))
    checks = [
        {
            "check_name": "latest_snapshot_row_count_meets_expected",
            "sql": """
                SELECT
                    CASE
                        WHEN COALESCE(latest.row_count, 0) >= %s THEN 0
                        ELSE %s - COALESCE(latest.row_count, 0)
                    END AS failed_row_count
                FROM (
                    SELECT COUNT(*) AS row_count
                    FROM mart_top_coins_latest
                    WHERE snapshot_ts_utc = (
                        SELECT MAX(snapshot_ts_utc) FROM mart_top_coins_latest
                    )
                ) latest
            """,
            "params": (expected_top_n, expected_top_n),
            "detail": {"expected_top_n": expected_top_n},
        },
        {
            "check_name": "latest_snapshot_rank_in_expected_range",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_top_coins_latest
                WHERE snapshot_ts_utc = (
                    SELECT MAX(snapshot_ts_utc) FROM mart_top_coins_latest
                )
                  AND (market_cap_rank IS NULL OR market_cap_rank < 1 OR market_cap_rank > %s)
            """,
            "params": (expected_top_n,),
            "detail": {"expected_top_n": expected_top_n},
        },
        {
            "check_name": "latest_snapshot_price_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_top_coins_latest
                WHERE snapshot_ts_utc = (
                    SELECT MAX(snapshot_ts_utc) FROM mart_top_coins_latest
                )
                  AND current_price < 0
            """,
        },
    ]
    return run_and_log_layer_business_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="mart",
        target_table="mart_top_coins_latest",
        checks=checks,
    )


def run_and_log_mart_market_summary_daily_business_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
) -> list[dict[str, Any]]:
    checks = [
        {
            "check_name": "total_market_cap_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_market_summary_daily
                WHERE total_market_cap_usd < 0
            """,
        },
        {
            "check_name": "total_volume_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_market_summary_daily
                WHERE total_volume_usd < 0
            """,
        },
        {
            "check_name": "market_cap_pct_btc_in_range",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_market_summary_daily
                WHERE market_cap_percentage_btc IS NOT NULL
                  AND (market_cap_percentage_btc < 0 OR market_cap_percentage_btc > 100)
            """,
        },
        {
            "check_name": "market_cap_pct_eth_in_range",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_market_summary_daily
                WHERE market_cap_percentage_eth IS NOT NULL
                  AND (market_cap_percentage_eth < 0 OR market_cap_percentage_eth > 100)
            """,
        },
    ]
    return run_and_log_layer_business_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="mart",
        target_table="mart_market_summary_daily",
        checks=checks,
    )


def run_and_log_mart_coin_daily_metrics_business_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
) -> list[dict[str, Any]]:
    checks = [
        {
            "check_name": "price_bounds_consistent",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_coin_daily_metrics
                WHERE max_price < min_price
            """,
        },
        {
            "check_name": "close_price_within_min_max",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_coin_daily_metrics
                WHERE close_price IS NOT NULL
                  AND min_price IS NOT NULL
                  AND max_price IS NOT NULL
                  AND (close_price < min_price OR close_price > max_price)
            """,
        },
        {
            "check_name": "non_negative_prices_and_volume",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_coin_daily_metrics
                WHERE (close_price IS NOT NULL AND close_price < 0)
                   OR (avg_price IS NOT NULL AND avg_price < 0)
                   OR (max_price IS NOT NULL AND max_price < 0)
                   OR (min_price IS NOT NULL AND min_price < 0)
                   OR (total_volume IS NOT NULL AND total_volume < 0)
            """,
        },
    ]
    return run_and_log_layer_business_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="mart",
        target_table="mart_coin_daily_metrics",
        checks=checks,
    )


def run_and_log_mart_coin_rolling_metrics_business_checks(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
) -> list[dict[str, Any]]:
    checks = [
        {
            "check_name": "rolling_metrics_non_negative",
            "sql": """
                SELECT COUNT(*) AS failed_row_count
                FROM mart_coin_rolling_metrics
                WHERE (close_price IS NOT NULL AND close_price < 0)
                   OR (sma_7d IS NOT NULL AND sma_7d < 0)
                   OR (sma_30d IS NOT NULL AND sma_30d < 0)
                   OR (avg_volume_7d IS NOT NULL AND avg_volume_7d < 0)
                   OR (avg_volume_30d IS NOT NULL AND avg_volume_30d < 0)
            """,
        },
    ]
    return run_and_log_layer_business_dq_checks(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        layer="mart",
        target_table="mart_coin_rolling_metrics",
        checks=checks,
    )
