from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.observability import evaluate_ops_alerts  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Cek alert operasional dari crypto_ops "
            "(pipeline_run failed dan DQ failed) untuk lookback tertentu."
        )
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=None,
        help="Window lookback dalam jam. Default: OPS_ALERT_LOOKBACK_HOURS atau 6.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Maksimum detail alert yang ditampilkan per kategori. Default: OPS_ALERT_MAX_ITEMS atau 20.",
    )
    parser.add_argument(
        "--exclude-dag-id",
        action="append",
        default=None,
        help=(
            "DAG yang dikecualikan dari alert. Bisa diulang beberapa kali. "
            "Default mengikuti OPS_ALERT_EXCLUDE_DAG_IDS."
        ),
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    report = evaluate_ops_alerts(
        lookback_hours=args.lookback_hours,
        max_items=args.max_items,
        excluded_dag_ids=args.exclude_dag_id,
    )

    print("[OPS ALERT REPORT]")
    print(f"  lookback_hours={report['lookback_hours']}")
    print(f"  window_start_utc={report['window_start_utc']}")
    print(f"  excluded_dag_ids={report['excluded_dag_ids']}")
    print(f"  failed_pipeline_run_count={report['failed_pipeline_run_count']}")
    print(f"  failed_dq_check_count={report['failed_dq_check_count']}")

    if report["failed_pipeline_runs"]:
        print("\n[FAILED PIPELINE RUNS]")
        for row in report["failed_pipeline_runs"]:
            print(
                f"  dag_id={row['dag_id']} dag_run_id={row['dag_run_id']} "
                f"started_at_utc={row['started_at_utc']} message={row['message']}"
            )

    if report["failed_dq_checks"]:
        print("\n[FAILED DQ CHECKS]")
        for row in report["failed_dq_checks"]:
            print(
                f"  dag_id={row['dag_id']} dag_run_id={row['dag_run_id']} "
                f"task_id={row['task_id']} target_table={row['target_table']} "
                f"check_name={row['check_name']} failed_row_count={row['failed_row_count']}"
            )

    if report["has_alert"]:
        print("\n[ALERT] Ditemukan anomali operasional di crypto_ops.")
        return 1

    print("\n[OK] Tidak ada alert operasional pada window ini.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
