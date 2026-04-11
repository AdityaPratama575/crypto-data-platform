from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from src.clients import CoinGeckoClient


SOURCE_ENDPOINT = "/global"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_batch_id(prefix: str) -> str:
    timestamp = utc_now().strftime("%Y%m%dT%H%M%S")
    return f"{prefix}_{timestamp}_{uuid4().hex[:8]}"


def extract_global_metrics(
    *,
    dag_run_id: str | None = None,
    task_run_id: str | None = None,
    logical_run_ts_utc: datetime | None = None,
) -> dict:
    extracted_at_utc = utc_now()
    logical_ts = logical_run_ts_utc or extracted_at_utc

    with CoinGeckoClient() as client:
        payload = client.get_global()

    return {
        "batch_id": make_batch_id("global"),
        "dag_run_id": dag_run_id,
        "task_run_id": task_run_id,
        "logical_run_ts_utc": logical_ts,
        "extracted_at_utc": extracted_at_utc,
        "snapshot_ts_utc": extracted_at_utc,
        "source_endpoint": SOURCE_ENDPOINT,
        "request_params": {},
        "payload": payload,
    }
