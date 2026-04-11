from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.extract import (  # noqa: E402
    extract_coin_history_top_n,
    extract_coin_list,
    extract_coin_markets_top_n,
    extract_global_metrics,
)
from src.load import (  # noqa: E402
    load_coin_market_chart_point_rows,
    load_coin_list_snapshot_rows,
    load_coin_markets_snapshot_rows,
    load_global_snapshot_rows,
    write_raw_payload,
)


def ensure_env_file() -> None:
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        raise FileNotFoundError(
            f"File .env tidak ditemukan di {env_path}. Buat dulu dari .env.example."
        )
    load_dotenv(env_path, override=False)


def run_coin_list_flow() -> dict[str, str | int]:
    snapshot = extract_coin_list(task_run_id="manual_coin_list")
    raw_file = write_raw_payload(
        endpoint_name="coins_list",
        batch_id=snapshot["batch_id"],
        source_endpoint=snapshot["source_endpoint"],
        request_params=snapshot["request_params"],
        payload=snapshot["payload"],
        logical_run_ts_utc=snapshot["logical_run_ts_utc"],
        extracted_at_utc=snapshot["extracted_at_utc"],
    )
    inserted_rows = load_coin_list_snapshot_rows(snapshot)
    return {
        "flow": "coin_list",
        "batch_id": snapshot["batch_id"],
        "raw_file": str(raw_file),
        "payload_count": len(snapshot["payload"]),
        "inserted_rows": inserted_rows,
    }


def run_coin_markets_flow() -> dict[str, str | int]:
    snapshot = extract_coin_markets_top_n(task_run_id="manual_coin_markets")
    raw_file = write_raw_payload(
        endpoint_name="coins_markets",
        batch_id=snapshot["batch_id"],
        source_endpoint=snapshot["source_endpoint"],
        request_params=snapshot["request_params"],
        payload=snapshot["payload"],
        logical_run_ts_utc=snapshot["logical_run_ts_utc"],
        extracted_at_utc=snapshot["extracted_at_utc"],
    )
    inserted_rows = load_coin_markets_snapshot_rows(snapshot)
    return {
        "flow": "coin_markets",
        "batch_id": snapshot["batch_id"],
        "raw_file": str(raw_file),
        "payload_count": len(snapshot["payload"]),
        "inserted_rows": inserted_rows,
    }


def run_global_flow() -> dict[str, str | int]:
    snapshot = extract_global_metrics(task_run_id="manual_global")
    raw_file = write_raw_payload(
        endpoint_name="global",
        batch_id=snapshot["batch_id"],
        source_endpoint=snapshot["source_endpoint"],
        request_params=snapshot["request_params"],
        payload=snapshot["payload"],
        logical_run_ts_utc=snapshot["logical_run_ts_utc"],
        extracted_at_utc=snapshot["extracted_at_utc"],
    )
    inserted_rows = load_global_snapshot_rows(snapshot)
    return {
        "flow": "global",
        "batch_id": snapshot["batch_id"],
        "raw_file": str(raw_file),
        "payload_count": 1,
        "inserted_rows": inserted_rows,
    }


def run_coin_history_flow(*, top_n: int | None = None, days: int = 1) -> dict[str, str | int]:
    snapshot = extract_coin_history_top_n(
        top_n=top_n,
        days=days,
        task_run_id="manual_coin_history",
    )

    raw_file_count = 0
    for coin_payload in snapshot["coin_payloads"]:
        coin_id = coin_payload["coin_id"]
        write_raw_payload(
            endpoint_name="coin_market_chart",
            batch_id=snapshot["batch_id"],
            source_endpoint=snapshot["source_endpoint"],
            request_params=coin_payload["request_params"],
            payload=coin_payload["payload"],
            logical_run_ts_utc=snapshot["logical_run_ts_utc"],
            extracted_at_utc=snapshot["extracted_at_utc"],
            file_suffix=coin_id,
        )
        raw_file_count += 1

    inserted_rows = load_coin_market_chart_point_rows(snapshot)
    return {
        "flow": "coin_history",
        "batch_id": snapshot["batch_id"],
        "raw_file": f"data/raw/coin_market_chart/* ({raw_file_count} files)",
        "payload_count": len(snapshot["coin_payloads"]),
        "inserted_rows": inserted_rows,
    }


def print_summary(result: dict[str, str | int]) -> None:
    print(f"[FLOW] {result['flow']}")
    print(f"  batch_id={result['batch_id']}")
    print(f"  raw_file={result['raw_file']}")
    print(f"  payload_count={result['payload_count']}")
    print(f"  inserted_rows={result['inserted_rows']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Jalankan extract + raw write + raw load tanpa Airflow."
    )
    parser.add_argument(
        "--flow",
        choices=["coin_list", "coin_markets", "global", "coin_history", "all"],
        default="all",
        help="Pilih flow yang dijalankan.",
    )
    parser.add_argument(
        "--history-top-n",
        type=int,
        default=None,
        help="Jumlah tracked coins untuk flow coin_history. Default: TRACKED_COINS_TOP_N.",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=1,
        help="Jumlah hari history untuk flow coin_history. Default: 1.",
    )
    return parser.parse_args()


def run() -> None:
    ensure_env_file()
    args = parse_args()

    if args.flow == "coin_list":
        print_summary(run_coin_list_flow())
        return

    if args.flow == "coin_markets":
        print_summary(run_coin_markets_flow())
        return

    if args.flow == "global":
        print_summary(run_global_flow())
        return

    if args.flow == "coin_history":
        print_summary(run_coin_history_flow(top_n=args.history_top_n, days=args.history_days))
        return

    print_summary(run_coin_list_flow())
    print_summary(run_coin_markets_flow())
    print_summary(run_global_flow())
    print_summary(run_coin_history_flow(top_n=args.history_top_n, days=args.history_days))


if __name__ == "__main__":
    try:
        run()
        print("[DONE] Local extract selesai.")
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc
