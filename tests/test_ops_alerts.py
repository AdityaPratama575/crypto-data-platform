from __future__ import annotations

from contextlib import contextmanager

import pytest

from src.observability import alerts


class _FakeCursor:
    def __init__(
        self,
        responses: list[list[dict]],
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

    def fetchall(self) -> list[dict]:
        if not self._responses:
            raise AssertionError("fetchall dipanggil melebihi response mock.")
        return self._responses.pop(0)


class _FakeConnection:
    def __init__(
        self,
        responses: list[list[dict]],
        executed_sql: list[tuple[str, tuple | None]],
    ) -> None:
        self._responses = responses
        self._executed_sql = executed_sql

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._responses, self._executed_sql)


def test_evaluate_ops_alerts_no_alert(monkeypatch) -> None:
    responses = [
        [],
        [],
    ]
    executed_sql: list[tuple[str, tuple | None]] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "ops"
        yield _FakeConnection(responses, executed_sql)

    monkeypatch.setattr(alerts, "mysql_connection", fake_mysql_connection)

    report = alerts.evaluate_ops_alerts(
        lookback_hours=6,
        max_items=5,
        excluded_dag_ids=["ops_alerting_dag"],
    )

    assert report["has_alert"] is False
    assert report["failed_pipeline_run_count"] == 0
    assert report["failed_dq_check_count"] == 0
    assert len(executed_sql) == 2
    assert executed_sql[0][1][-1] == 5
    assert "ops_alerting_dag" in executed_sql[0][1]
    assert executed_sql[1][1][-1] == 5


def test_assert_no_ops_alerts_raises_when_alert_exists(monkeypatch) -> None:
    responses = [
        [{"dag_id": "coin_history_daily_dag"}],
        [],
    ]

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "ops"
        yield _FakeConnection(responses, [])

    monkeypatch.setattr(alerts, "mysql_connection", fake_mysql_connection)

    with pytest.raises(RuntimeError):
        alerts.assert_no_ops_alerts(lookback_hours=6, max_items=10)
