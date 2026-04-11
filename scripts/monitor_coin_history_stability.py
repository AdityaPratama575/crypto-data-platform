from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db import mysql_connection  # noqa: E402


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _classify_failure(message: str | None) -> str:
    if not message:
        return "unknown"

    normalized = message.lower()
    if "429" in normalized:
        return "rate_limit_429"
    if "dq raw gagal" in normalized:
        return "dq_raw_failure"
    if "doesn't exist" in normalized or "does not exist" in normalized:
        return "schema_or_table_missing"
    return "other"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pantau stabilitas coin_history_daily_dag dari crypto_ops "
            "dan berikan rekomendasi tuning COIN_HISTORY_*."
        )
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Window observasi dalam hari (default: 7).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=200,
        help="Maksimum rows detail task_run yang dianalisis (default: 200).",
    )
    return parser.parse_args()


def _build_recommendation(failure_categories: Counter[str]) -> dict[str, str]:
    recommendation = {
        "COIN_HISTORY_BATCH_SIZE": "10",
        "COIN_HISTORY_REQUEST_PAUSE_SECONDS": "1.5",
        "COIN_HISTORY_BATCH_PAUSE_SECONDS": "8.0",
        "COIN_HISTORY_MAX_RETRIES": "6",
        "COIN_HISTORY_BACKOFF_SECONDS": "4",
    }

    if failure_categories["rate_limit_429"] == 0:
        recommendation["COIN_HISTORY_REQUEST_PAUSE_SECONDS"] = "1.2"
        recommendation["COIN_HISTORY_BATCH_PAUSE_SECONDS"] = "6.0"

    return recommendation


def run() -> int:
    args = parse_args()
    if args.lookback_days <= 0:
        raise ValueError("lookback_days harus lebih besar dari 0.")
    if args.max_rows <= 0:
        raise ValueError("max_rows harus lebih besar dari 0.")

    window_start_utc = _utc_now_naive() - timedelta(days=args.lookback_days)

    with mysql_connection(layer="ops") as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_status, COUNT(*) AS run_count
                FROM pipeline_run
                WHERE dag_id = %s
                  AND started_at_utc >= %s
                GROUP BY run_status
                """,
                ("coin_history_daily_dag", window_start_utc),
            )
            run_status_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    dag_run_id,
                    task_status,
                    rows_extracted,
                    rows_loaded,
                    error_message,
                    started_at_utc,
                    ended_at_utc
                FROM task_run
                WHERE dag_id = %s
                  AND started_at_utc >= %s
                ORDER BY task_run_id DESC
                LIMIT %s
                """,
                ("coin_history_daily_dag", window_start_utc, args.max_rows),
            )
            task_rows = cursor.fetchall()

    run_counts = {row["run_status"]: int(row["run_count"]) for row in run_status_rows}
    success_count = run_counts.get("success", 0)
    failed_count = run_counts.get("failed", 0)
    total_count = success_count + failed_count
    success_rate = (success_count / total_count) if total_count > 0 else 0.0

    failed_task_rows = [row for row in task_rows if row["task_status"] == "failed"]
    failure_categories = Counter(_classify_failure(row.get("error_message")) for row in failed_task_rows)
    recommendation = _build_recommendation(failure_categories)

    print("[COIN_HISTORY STABILITY REPORT]")
    print(f"  lookback_days={args.lookback_days}")
    print(f"  window_start_utc={window_start_utc}")
    print(f"  total_runs={total_count}")
    print(f"  success_runs={success_count}")
    print(f"  failed_runs={failed_count}")
    print(f"  success_rate={success_rate:.2%}")

    print("\n[FAILURE CATEGORIES]")
    if not failure_categories:
        print("  none")
    else:
        for category, count in failure_categories.items():
            print(f"  {category}={count}")

    print("\n[RECOMMENDED COIN_HISTORY_*]")
    for key, value in recommendation.items():
        print(f"  {key}={value}")

    print("\n[NOTE]")
    print("  Terapkan rekomendasi bertahap, lalu monitor ulang 3-7 hari.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
