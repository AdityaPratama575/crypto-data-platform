from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DATA_DIR = ROOT_DIR / "data" / "raw"
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


def get_raw_data_dir() -> Path:
    load_environment()
    configured = os.getenv("RAW_DATA_DIR")
    if not configured:
        return DEFAULT_RAW_DATA_DIR

    raw_dir = Path(configured)
    if raw_dir.is_absolute():
        return raw_dir
    return ROOT_DIR / raw_dir


def write_raw_payload(
    *,
    endpoint_name: str,
    batch_id: str,
    source_endpoint: str,
    request_params: dict[str, Any] | None,
    payload: Any,
    logical_run_ts_utc: datetime,
    extracted_at_utc: datetime | None = None,
    file_suffix: str | None = None,
) -> Path:
    extracted_at = extracted_at_utc or utc_now()
    endpoint_dir = get_raw_data_dir() / endpoint_name
    endpoint_dir.mkdir(parents=True, exist_ok=True)

    suffix_part = f"_{file_suffix}" if file_suffix else ""
    file_name = f"{extracted_at.strftime('%Y%m%dT%H%M%S%fZ')}_{batch_id}{suffix_part}.json"
    file_path = endpoint_dir / file_name

    envelope = {
        "batch_id": batch_id,
        "source_endpoint": source_endpoint,
        "request_params": request_params,
        "logical_run_ts_utc": logical_run_ts_utc.isoformat(),
        "extracted_at_utc": extracted_at.isoformat(),
        "payload": payload,
    }
    file_path.write_text(json.dumps(envelope, ensure_ascii=False), encoding="utf-8")
    return file_path
