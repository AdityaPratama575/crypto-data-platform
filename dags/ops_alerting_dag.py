from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from pendulum import datetime


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.observability import assert_no_ops_alerts  # noqa: E402


def _parse_excluded_dag_ids_from_env() -> list[str]:
    raw_value = os.getenv("OPS_ALERT_EXCLUDE_DAG_IDS", "")
    return [item.strip() for item in raw_value.split(",") if item.strip()]


@dag(
    dag_id="ops_alerting_dag",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "crypto-data-platform", "retries": 0},
    tags=["crypto", "ops", "alert", "v1"],
)
def ops_alerting_pipeline():
    @task(task_id="check_ops_alerts")
    def check_ops_alerts(**context: Any) -> dict[str, Any]:
        dag_run = context.get("dag_run")
        run_conf: dict[str, Any] = {}
        if dag_run and isinstance(dag_run.conf, dict):
            run_conf = dag_run.conf

        lookback_hours: int | None = None
        max_items: int | None = None
        excluded_dag_ids: list[str] | None = None

        if run_conf.get("lookback_hours") is not None:
            lookback_hours = int(run_conf["lookback_hours"])
        if run_conf.get("max_items") is not None:
            max_items = int(run_conf["max_items"])
        if isinstance(run_conf.get("excluded_dag_ids"), list):
            excluded_dag_ids = [str(item) for item in run_conf["excluded_dag_ids"]]
        elif run_conf.get("excluded_dag_ids") is not None:
            excluded_dag_ids = [
                item.strip()
                for item in str(run_conf["excluded_dag_ids"]).split(",")
                if item.strip()
            ]

        if excluded_dag_ids is None:
            excluded_dag_ids = _parse_excluded_dag_ids_from_env()

        # Hindari self-referential noise agar DAG alert tidak alerting dirinya sendiri.
        if "ops_alerting_dag" not in excluded_dag_ids:
            excluded_dag_ids.append("ops_alerting_dag")

        report = assert_no_ops_alerts(
            lookback_hours=lookback_hours,
            max_items=max_items,
            excluded_dag_ids=excluded_dag_ids,
        )
        summary = {
            "lookback_hours": report["lookback_hours"],
            "window_start_utc": str(report["window_start_utc"]),
            "failed_pipeline_run_count": report["failed_pipeline_run_count"],
            "failed_dq_check_count": report["failed_dq_check_count"],
            "excluded_dag_ids": report["excluded_dag_ids"],
        }
        print(f"[INFO] ops_alerting summary: {summary}")
        return summary

    check_ops_alerts()


ops_alerting_dag = ops_alerting_pipeline()
