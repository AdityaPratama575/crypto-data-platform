from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from src.quality import checks


class _FakeCursor:
    def __init__(
        self,
        responses: list[dict[str, int]],
        executed_sql: list[tuple[str, tuple | None]],
    ) -> None:
        self._responses = responses
        self._executed_sql = executed_sql

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:  # noqa: ANN001
        return False

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self._executed_sql.append((sql, params))

    def fetchone(self) -> dict[str, int]:
        if not self._responses:
            raise AssertionError("fetchone dipanggil melebihi jumlah response mock.")
        return self._responses.pop(0)


class _FakeConnection:
    def __init__(
        self,
        responses: list[dict[str, int]],
        executed_sql: list[tuple[str, tuple | None]],
    ) -> None:
        self._responses = responses
        self._executed_sql = executed_sql

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._responses, self._executed_sql)


def test_run_and_log_raw_batch_dq_checks_all_pass(monkeypatch) -> None:
    executed_sql: list[tuple[str, tuple | None]] = []
    responses = [
        {"row_count": 3},
        {"failed_row_count": 0},
        {"failed_row_count": 0},
        {"failed_row_count": 0},
    ]
    inserted_results: list[dict] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "raw"
        yield _FakeConnection(responses, executed_sql)

    def fake_insert_dq_check_result(**kwargs) -> None:  # noqa: ANN003
        inserted_results.append(kwargs)

    monkeypatch.setattr(checks, "mysql_connection", fake_mysql_connection)
    monkeypatch.setattr(checks, "insert_dq_check_result", fake_insert_dq_check_result)
    monkeypatch.setattr(
        checks,
        "utc_now",
        lambda: datetime(2026, 4, 5, 10, 0, 0, tzinfo=timezone.utc),
    )

    results = checks.run_and_log_raw_batch_dq_checks(
        dag_id="demo_dag",
        dag_run_id="demo_run",
        task_id="demo_task",
        target_table="raw_coin_markets_snapshot",
        batch_id="batch_1",
        required_not_null_columns=["coin_id", "raw_payload_json"],
        unique_key_columns=["coin_id"],
    )

    assert len(results) == 4
    assert all(result["check_status"] == "pass" for result in results)
    assert len(inserted_results) == 4
    assert len(executed_sql) == 4
    assert inserted_results[0]["check_name"] == "row_count_positive"


def test_run_and_log_raw_batch_dq_checks_invalid_identifier() -> None:
    with pytest.raises(ValueError):
        checks.run_and_log_raw_batch_dq_checks(
            dag_id="demo_dag",
            dag_run_id="demo_run",
            task_id="demo_task",
            target_table="raw_coin_markets_snapshot;DROP TABLE x",
            batch_id="batch_1",
            required_not_null_columns=["coin_id"],
            unique_key_columns=None,
        )


def test_run_and_log_core_batch_dq_checks_uses_source_batch_filter(monkeypatch) -> None:
    executed_sql: list[tuple[str, tuple | None]] = []
    responses = [
        {"row_count": 10},
        {"failed_row_count": 0},
        {"failed_row_count": 0},
    ]
    inserted_results: list[dict] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "core"
        yield _FakeConnection(responses, executed_sql)

    def fake_insert_dq_check_result(**kwargs) -> None:  # noqa: ANN003
        inserted_results.append(kwargs)

    monkeypatch.setattr(checks, "mysql_connection", fake_mysql_connection)
    monkeypatch.setattr(checks, "insert_dq_check_result", fake_insert_dq_check_result)
    monkeypatch.setattr(
        checks,
        "utc_now",
        lambda: datetime(2026, 4, 6, 11, 0, 0, tzinfo=timezone.utc),
    )

    results = checks.run_and_log_core_batch_dq_checks(
        dag_id="demo_dag",
        dag_run_id="demo_run",
        task_id="demo_task",
        target_table="fact_coin_market_snapshot",
        source_batch_id="batch_core_1",
        required_not_null_columns=["coin_id"],
        unique_key_columns=["snapshot_ts_utc", "coin_id", "vs_currency"],
    )

    assert len(results) == 3
    assert all(result["check_status"] == "pass" for result in results)
    assert len(inserted_results) == 3
    assert len(executed_sql) == 3
    assert executed_sql[0][1] == ("batch_core_1",)


def test_run_and_log_mart_table_dq_checks_without_filter(monkeypatch) -> None:
    executed_sql: list[tuple[str, tuple | None]] = []
    responses = [
        {"row_count": 2},
        {"failed_row_count": 0},
        {"failed_row_count": 0},
    ]
    inserted_results: list[dict] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "mart"
        yield _FakeConnection(responses, executed_sql)

    def fake_insert_dq_check_result(**kwargs) -> None:  # noqa: ANN003
        inserted_results.append(kwargs)

    monkeypatch.setattr(checks, "mysql_connection", fake_mysql_connection)
    monkeypatch.setattr(checks, "insert_dq_check_result", fake_insert_dq_check_result)
    monkeypatch.setattr(
        checks,
        "utc_now",
        lambda: datetime(2026, 4, 6, 11, 5, 0, tzinfo=timezone.utc),
    )

    results = checks.run_and_log_mart_table_dq_checks(
        dag_id="demo_dag",
        dag_run_id="demo_run",
        task_id="demo_task",
        target_table="mart_top_coins_latest",
        required_not_null_columns=["coin_id"],
        unique_key_columns=["snapshot_ts_utc", "coin_id", "vs_currency"],
    )

    assert len(results) == 3
    assert all(result["check_status"] == "pass" for result in results)
    assert len(inserted_results) == 3
    assert len(executed_sql) == 3
    assert "batch_id" not in executed_sql[0][0]


def test_run_and_log_layer_business_dq_checks_logs_pass_and_fail(monkeypatch) -> None:
    executed_sql: list[tuple[str, tuple | None]] = []
    responses = [
        {"failed_row_count": 0},
        {"failed_row_count": 2},
    ]
    inserted_results: list[dict] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "core"
        yield _FakeConnection(responses, executed_sql)

    def fake_insert_dq_check_result(**kwargs) -> None:  # noqa: ANN003
        inserted_results.append(kwargs)

    monkeypatch.setattr(checks, "mysql_connection", fake_mysql_connection)
    monkeypatch.setattr(checks, "insert_dq_check_result", fake_insert_dq_check_result)
    monkeypatch.setattr(
        checks,
        "utc_now",
        lambda: datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc),
    )

    results = checks.run_and_log_layer_business_dq_checks(
        dag_id="demo_dag",
        dag_run_id="demo_run",
        task_id="demo_task",
        layer="core",
        target_table="fact_coin_market_snapshot",
        checks=[
            {"check_name": "a", "sql": "SELECT 0 AS failed_row_count"},
            {"check_name": "b", "sql": "SELECT 2 AS failed_row_count"},
        ],
    )

    assert len(results) == 2
    assert results[0]["check_status"] == "pass"
    assert results[1]["check_status"] == "fail"
    assert len(inserted_results) == 2
    assert len(executed_sql) == 2


def test_run_and_log_mart_top_coins_latest_business_checks_uses_expected_top_n(
    monkeypatch,
) -> None:
    executed_sql: list[tuple[str, tuple | None]] = []
    responses = [
        {"failed_row_count": 0},
        {"failed_row_count": 0},
        {"failed_row_count": 0},
    ]
    inserted_results: list[dict] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "mart"
        yield _FakeConnection(responses, executed_sql)

    def fake_insert_dq_check_result(**kwargs) -> None:  # noqa: ANN003
        inserted_results.append(kwargs)

    monkeypatch.setattr(checks, "mysql_connection", fake_mysql_connection)
    monkeypatch.setattr(checks, "insert_dq_check_result", fake_insert_dq_check_result)
    monkeypatch.setenv("TRACKED_COINS_TOP_N", "150")
    monkeypatch.setattr(
        checks,
        "utc_now",
        lambda: datetime(2026, 4, 6, 12, 5, 0, tzinfo=timezone.utc),
    )

    results = checks.run_and_log_mart_top_coins_latest_business_checks(
        dag_id="demo_dag",
        dag_run_id="demo_run",
        task_id="demo_task",
    )

    assert len(results) == 3
    assert all(result["check_status"] == "pass" for result in results)
    assert len(inserted_results) == 3
    assert len(executed_sql) == 3
    assert executed_sql[0][1] == (150, 150)
    assert executed_sql[1][1] == (150,)
