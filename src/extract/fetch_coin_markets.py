from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv

from src.clients import CoinGeckoClient


ROOT_DIR = Path(__file__).resolve().parents[2]
SOURCE_ENDPOINT = "/coins/markets"
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


def extract_coin_markets_top_n(
    *,
    top_n: int | None = None,
    vs_currency: str | None = None,
    order: str = "market_cap_desc",
    dag_run_id: str | None = None,
    task_run_id: str | None = None,
    logical_run_ts_utc: datetime | None = None,
) -> dict:
    load_environment()
    limit = top_n or env_int("TRACKED_COINS_TOP_N", 100)
    if limit <= 0:
        raise ValueError("top_n harus lebih besar dari 0.")
    if limit > 250:
        raise ValueError("top_n maksimal 250 untuk satu request CoinGecko /coins/markets.")

    selected_vs_currency = vs_currency or os.getenv("COINGECKO_VS_CURRENCY", "usd")
    extracted_at_utc = utc_now()
    logical_ts = logical_run_ts_utc or extracted_at_utc
    request_params = {
        "vs_currency": selected_vs_currency,
        "order": order,
        "per_page": limit,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    with CoinGeckoClient() as client:
        payload = client.get_coin_markets(
            vs_currency=selected_vs_currency,
            order=order,
            per_page=limit,
            page=1,
            sparkline=False,
            price_change_percentage="24h",
        )

    return {
        "batch_id": make_batch_id("coin_markets"),
        "dag_run_id": dag_run_id,
        "task_run_id": task_run_id,
        "logical_run_ts_utc": logical_ts,
        "extracted_at_utc": extracted_at_utc,
        "snapshot_ts_utc": extracted_at_utc,
        "source_endpoint": SOURCE_ENDPOINT,
        "request_params": request_params,
        "payload": payload,
        "vs_currency": selected_vs_currency,
        "page_no": 1,
    }
