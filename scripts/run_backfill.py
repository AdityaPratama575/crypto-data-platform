from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone


SUPPORTED_DAGS = [
    "reference_coin_list_dag",
    "market_snapshot_hourly_dag",
    "global_market_hourly_dag",
    "coin_history_daily_dag",
    "ops_alerting_dag",
]


def _default_run_id(dag_id: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"backfill_{dag_id}_{ts}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trigger DAG backfill manual via container Airflow scheduler."
    )
    parser.add_argument(
        "--dag-id",
        required=True,
        choices=SUPPORTED_DAGS,
        help="DAG yang akan di-trigger.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Run ID manual. Jika kosong, otomatis backfill_<dag>_<timestamp UTC>.",
    )
    parser.add_argument(
        "--conf",
        default=None,
        help="JSON string untuk dag_run.conf (opsional).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Tampilkan command tanpa eksekusi.",
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    run_id = args.run_id or _default_run_id(args.dag_id)

    command = [
        "docker",
        "exec",
        "crypto_airflow_scheduler",
        "airflow",
        "dags",
        "trigger",
        args.dag_id,
        "--run-id",
        run_id,
    ]

    if args.conf:
        try:
            parsed_conf = json.loads(args.conf)
        except json.JSONDecodeError as exc:
            raise ValueError("--conf harus JSON valid.") from exc
        normalized_conf = json.dumps(parsed_conf, separators=(",", ":"))
        command.extend(["--conf", normalized_conf])

    print("[INFO] Command:")
    print("  " + " ".join(command))

    if args.dry_run:
        print("[INFO] Dry run mode, command tidak dieksekusi.")
        return 0

    subprocess.run(command, check=True)
    print("[DONE] Trigger backfill sukses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
