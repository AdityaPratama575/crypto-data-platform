from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from src.db import mysql_connection


def _naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    return ts.astimezone(timezone.utc).replace(tzinfo=None)


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _parse_coingecko_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if not isinstance(value, str):
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _parse_epoch_to_utc_naive(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        epoch = int(value)
    except (TypeError, ValueError):
        return None

    # CoinGecko market_chart memakai millisecond epoch.
    # Presisi ms harus dipertahankan agar tidak collapse jadi timestamp detik yang sama.
    epoch_seconds = float(epoch)
    if abs(epoch) > 10**11:
        epoch_seconds = epoch / 1000.0

    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).replace(tzinfo=None)


def _series_to_map(series: Any) -> dict[int, Any]:
    mapped: dict[int, Any] = {}
    if not isinstance(series, list):
        return mapped

    for point in series:
        if not isinstance(point, list) or len(point) < 2:
            continue
        ts_raw, value = point[0], point[1]
        try:
            ts_ms = int(ts_raw)
        except (TypeError, ValueError):
            continue
        mapped[ts_ms] = value

    return mapped


def load_coin_list_snapshot_rows(snapshot: dict[str, Any]) -> int:
    payload = snapshot["payload"]
    request_params_json = json.dumps(snapshot.get("request_params"))
    rows = []

    for item in payload:
        rows.append(
            (
                snapshot["batch_id"],
                snapshot.get("dag_run_id"),
                snapshot.get("task_run_id"),
                _naive_utc(snapshot["logical_run_ts_utc"]),
                _naive_utc(snapshot["extracted_at_utc"]),
                snapshot["source_endpoint"],
                request_params_json,
                json.dumps(item),
                item.get("id"),
                item.get("symbol"),
                item.get("name"),
                json.dumps(item.get("platforms")),
            )
        )

    if not rows:
        return 0

    sql = """
    INSERT INTO raw_coin_list_snapshot (
        batch_id,
        dag_run_id,
        task_run_id,
        logical_run_ts_utc,
        extracted_at_utc,
        source_endpoint,
        request_params_json,
        raw_payload_json,
        coin_id,
        symbol,
        coin_name,
        platform_json
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    with mysql_connection(layer="raw") as connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, rows)
            inserted = cursor.rowcount

    return inserted


def load_coin_markets_snapshot_rows(snapshot: dict[str, Any]) -> int:
    payload = snapshot["payload"]
    request_params_json = json.dumps(snapshot.get("request_params"))
    rows = []

    for item in payload:
        rows.append(
            (
                snapshot["batch_id"],
                snapshot.get("dag_run_id"),
                snapshot.get("task_run_id"),
                _naive_utc(snapshot["logical_run_ts_utc"]),
                _naive_utc(snapshot["extracted_at_utc"]),
                _naive_utc(snapshot["snapshot_ts_utc"]),
                snapshot["source_endpoint"],
                request_params_json,
                json.dumps(item),
                snapshot["vs_currency"],
                snapshot["page_no"],
                item.get("id"),
                item.get("symbol"),
                item.get("name"),
                _to_decimal(item.get("current_price")),
                _to_decimal(item.get("market_cap")),
                item.get("market_cap_rank"),
                _to_decimal(item.get("total_volume")),
                _to_decimal(item.get("high_24h")),
                _to_decimal(item.get("low_24h")),
                _to_decimal(item.get("price_change_24h")),
                _to_decimal(item.get("price_change_percentage_24h")),
                _to_decimal(item.get("circulating_supply")),
                _to_decimal(item.get("total_supply")),
                _to_decimal(item.get("max_supply")),
                _to_decimal(item.get("ath")),
                _to_decimal(item.get("atl")),
                _parse_coingecko_datetime(item.get("last_updated")),
            )
        )

    if not rows:
        return 0

    sql = """
    INSERT INTO raw_coin_markets_snapshot (
        batch_id,
        dag_run_id,
        task_run_id,
        logical_run_ts_utc,
        extracted_at_utc,
        snapshot_ts_utc,
        source_endpoint,
        request_params_json,
        raw_payload_json,
        vs_currency,
        page_no,
        coin_id,
        symbol,
        coin_name,
        current_price,
        market_cap,
        market_cap_rank,
        total_volume,
        high_24h,
        low_24h,
        price_change_24h,
        price_change_percentage_24h,
        circulating_supply,
        total_supply,
        max_supply,
        ath,
        atl,
        last_updated_utc
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    with mysql_connection(layer="raw") as connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, rows)
            inserted = cursor.rowcount

    return inserted


def load_coin_market_chart_point_rows(snapshot: dict[str, Any]) -> int:
    coin_payloads = snapshot.get("coin_payloads") or []
    rows = []

    for coin_payload in coin_payloads:
        coin_id = coin_payload.get("coin_id")
        payload = coin_payload.get("payload")
        request_params = coin_payload.get("request_params")
        if not coin_id or not isinstance(payload, dict):
            continue

        prices_map = _series_to_map(payload.get("prices"))
        market_caps_map = _series_to_map(payload.get("market_caps"))
        total_volumes_map = _series_to_map(payload.get("total_volumes"))
        all_timestamps = sorted(
            set(prices_map.keys())
            | set(market_caps_map.keys())
            | set(total_volumes_map.keys())
        )
        request_params_json = json.dumps(request_params)

        for ts_ms in all_timestamps:
            price_ts_utc = _parse_epoch_to_utc_naive(ts_ms)
            if price_ts_utc is None:
                continue

            rows.append(
                (
                    snapshot["batch_id"],
                    snapshot.get("dag_run_id"),
                    snapshot.get("task_run_id"),
                    _naive_utc(snapshot["logical_run_ts_utc"]),
                    _naive_utc(snapshot["extracted_at_utc"]),
                    snapshot["source_endpoint"],
                    request_params_json,
                    json.dumps(payload),
                    coin_id,
                    snapshot["vs_currency"],
                    price_ts_utc,
                    _to_decimal(prices_map.get(ts_ms)),
                    _to_decimal(market_caps_map.get(ts_ms)),
                    _to_decimal(total_volumes_map.get(ts_ms)),
                )
            )

    if not rows:
        return 0

    sql = """
    INSERT INTO raw_coin_market_chart_point (
        batch_id,
        dag_run_id,
        task_run_id,
        logical_run_ts_utc,
        extracted_at_utc,
        source_endpoint,
        request_params_json,
        raw_payload_json,
        coin_id,
        vs_currency,
        price_ts_utc,
        price_value,
        market_cap_value,
        total_volume_value
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    with mysql_connection(layer="raw") as connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, rows)
            inserted = cursor.rowcount

    return inserted


def load_global_snapshot_rows(snapshot: dict[str, Any]) -> int:
    payload = snapshot["payload"]
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise ValueError("Payload /global tidak memiliki field 'data' yang valid.")

    request_params_json = json.dumps(snapshot.get("request_params"))
    updated_at_raw = data.get("updated_at")
    updated_at_int: int | None = None
    if updated_at_raw is not None:
        try:
            updated_at_int = int(updated_at_raw)
        except (TypeError, ValueError):
            updated_at_int = None

    updated_at_dt = _parse_epoch_to_utc_naive(updated_at_int)
    snapshot_ts_utc = updated_at_dt or _naive_utc(snapshot["snapshot_ts_utc"])

    row = (
        snapshot["batch_id"],
        snapshot.get("dag_run_id"),
        snapshot.get("task_run_id"),
        _naive_utc(snapshot["logical_run_ts_utc"]),
        _naive_utc(snapshot["extracted_at_utc"]),
        snapshot_ts_utc,
        snapshot["source_endpoint"],
        request_params_json,
        json.dumps(payload),
        data.get("active_cryptocurrencies"),
        data.get("upcoming_icos"),
        data.get("ongoing_icos"),
        data.get("ended_icos"),
        data.get("markets"),
        _to_decimal((data.get("total_market_cap") or {}).get("usd")),
        _to_decimal((data.get("total_volume") or {}).get("usd")),
        _to_decimal((data.get("market_cap_percentage") or {}).get("btc")),
        _to_decimal((data.get("market_cap_percentage") or {}).get("eth")),
        _to_decimal(data.get("market_cap_change_percentage_24h_usd")),
        updated_at_int,
    )

    sql = """
    INSERT INTO raw_global_snapshot (
        batch_id,
        dag_run_id,
        task_run_id,
        logical_run_ts_utc,
        extracted_at_utc,
        snapshot_ts_utc,
        source_endpoint,
        request_params_json,
        raw_payload_json,
        active_cryptocurrencies,
        upcoming_icos,
        ongoing_icos,
        ended_icos,
        markets,
        total_market_cap_usd,
        total_volume_usd,
        market_cap_percentage_btc,
        market_cap_percentage_eth,
        market_cap_change_percentage_24h_usd,
        updated_at_epoch
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    with mysql_connection(layer="raw") as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, row)
            inserted = cursor.rowcount

    return inserted
