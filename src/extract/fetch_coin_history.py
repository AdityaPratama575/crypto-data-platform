from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv

from src.clients import CoinGeckoClient


ROOT_DIR = Path(__file__).resolve().parents[2]
SOURCE_ENDPOINT = "/coins/{id}/market_chart"
_ENV_LOADED = False


def load_environment() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    env_path = ROOT_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)
    _ENV_LOADED = True


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_batch_id(prefix: str) -> str:
    timestamp = utc_now().strftime("%Y%m%dT%H%M%S")
    return f"{prefix}_{timestamp}_{uuid4().hex[:8]}"


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def extract_coin_history_top_n(
    *,
    top_n: int | None = None,
    days: int = 1,
    vs_currency: str | None = None,
    interval: str | None = None,
    batch_size: int | None = None,
    request_pause_seconds: float | None = None,
    batch_pause_seconds: float | None = None,
    client_max_retries: int | None = None,
    client_backoff_seconds: int | None = None,
    dag_run_id: str | None = None,
    task_run_id: str | None = None,
    logical_run_ts_utc: datetime | None = None,
) -> dict[str, Any]:
    load_environment()
    limit = top_n or env_int("TRACKED_COINS_TOP_N", 100)
    if limit <= 0:
        raise ValueError("top_n harus lebih besar dari 0.")
    if limit > 250:
        raise ValueError("top_n maksimal 250 untuk satu halaman /coins/markets.")
    if days <= 0:
        raise ValueError("days harus lebih besar dari 0.")

    selected_batch_size = (
        batch_size if batch_size is not None else env_int("COIN_HISTORY_BATCH_SIZE", 10)
    )
    if selected_batch_size <= 0:
        raise ValueError("batch_size harus lebih besar dari 0.")

    selected_request_pause_seconds = (
        request_pause_seconds
        if request_pause_seconds is not None
        else env_float("COIN_HISTORY_REQUEST_PAUSE_SECONDS", 1.5)
    )
    if selected_request_pause_seconds < 0:
        raise ValueError("request_pause_seconds tidak boleh negatif.")

    selected_batch_pause_seconds = (
        batch_pause_seconds
        if batch_pause_seconds is not None
        else env_float("COIN_HISTORY_BATCH_PAUSE_SECONDS", 8.0)
    )
    if selected_batch_pause_seconds < 0:
        raise ValueError("batch_pause_seconds tidak boleh negatif.")

    selected_client_max_retries = (
        client_max_retries
        if client_max_retries is not None
        else env_int("COIN_HISTORY_MAX_RETRIES", max(env_int("MAX_RETRIES", 3), 6))
    )
    if selected_client_max_retries <= 0:
        raise ValueError("client_max_retries harus lebih besar dari 0.")

    selected_client_backoff_seconds = (
        client_backoff_seconds
        if client_backoff_seconds is not None
        else env_int("COIN_HISTORY_BACKOFF_SECONDS", max(env_int("BACKOFF_SECONDS", 2), 4))
    )
    if selected_client_backoff_seconds <= 0:
        raise ValueError("client_backoff_seconds harus lebih besar dari 0.")

    selected_vs_currency = vs_currency or os.getenv("COINGECKO_VS_CURRENCY", "usd")
    extracted_at_utc = utc_now()
    logical_ts = logical_run_ts_utc or extracted_at_utc

    with CoinGeckoClient(
        max_retries=selected_client_max_retries,
        backoff_seconds=selected_client_backoff_seconds,
    ) as client:
        markets = client.get_coin_markets(
            vs_currency=selected_vs_currency,
            order="market_cap_desc",
            per_page=limit,
            page=1,
            sparkline=False,
            price_change_percentage="24h",
        )
        coin_ids = [item.get("id") for item in markets if item.get("id")]

        coin_payloads: list[dict[str, Any]] = []
        total_coins = len(coin_ids)
        for index, coin_id in enumerate(coin_ids, start=1):
            request_params = {
                "coin_id": coin_id,
                "vs_currency": selected_vs_currency,
                "days": str(days),
                "interval": interval,
            }
            payload = client.get_coin_market_chart(
                coin_id=coin_id,
                vs_currency=selected_vs_currency,
                days=days,
                interval=interval,
            )
            coin_payloads.append(
                {
                    "coin_id": coin_id,
                    "request_params": request_params,
                    "payload": payload,
                }
            )

            if (
                index < total_coins
                and selected_request_pause_seconds > 0
            ):
                time.sleep(selected_request_pause_seconds)

            if (
                index < total_coins
                and index % selected_batch_size == 0
                and selected_batch_pause_seconds > 0
            ):
                time.sleep(selected_batch_pause_seconds)

    return {
        "batch_id": make_batch_id("coin_history"),
        "dag_run_id": dag_run_id,
        "task_run_id": task_run_id,
        "logical_run_ts_utc": logical_ts,
        "extracted_at_utc": extracted_at_utc,
        "source_endpoint": SOURCE_ENDPOINT,
        "vs_currency": selected_vs_currency,
        "days": days,
        "interval": interval,
        "batch_size": selected_batch_size,
        "request_pause_seconds": selected_request_pause_seconds,
        "batch_pause_seconds": selected_batch_pause_seconds,
        "client_max_retries": selected_client_max_retries,
        "client_backoff_seconds": selected_client_backoff_seconds,
        "coin_payloads": coin_payloads,
    }
