from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from src.transform import mart_retention


class _FakeCursor:
    def __init__(self, executed_sql: list[tuple[str, tuple | None]], rowcount: int) -> None:
        self._executed_sql = executed_sql
        self.rowcount = rowcount

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:  # noqa: ANN001
        return False

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self._executed_sql.append((sql, params))


class _FakeConnection:
    def __init__(self, executed_sql: list[tuple[str, tuple | None]], rowcount: int) -> None:
        self._executed_sql = executed_sql
        self._rowcount = rowcount

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._executed_sql, self._rowcount)


def test_cleanup_mart_top_coins_latest_disabled_by_default(monkeypatch) -> None:
    executed_sql: list[tuple[str, tuple | None]] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "mart"
        yield _FakeConnection(executed_sql, rowcount=99)

    monkeypatch.setattr(mart_retention, "mysql_connection", fake_mysql_connection)
    monkeypatch.delenv("MART_RETENTION_ENABLED", raising=False)

    result = mart_retention.cleanup_mart_top_coins_latest(
        now_utc=datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    )

    assert result["retention_enabled"] is False
    assert result["deleted_rows"] == 0
    assert executed_sql == []


def test_cleanup_mart_top_coins_latest_executes_delete_when_enabled(monkeypatch) -> None:
    executed_sql: list[tuple[str, tuple | None]] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "mart"
        yield _FakeConnection(executed_sql, rowcount=7)

    monkeypatch.setattr(mart_retention, "mysql_connection", fake_mysql_connection)
    monkeypatch.setenv("MART_RETENTION_ENABLED", "true")
    monkeypatch.setenv("MART_TOP_COINS_RETENTION_DAYS", "30")

    result = mart_retention.cleanup_mart_top_coins_latest(
        now_utc=datetime(2026, 4, 6, 0, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    )

    assert result["retention_enabled"] is True
    assert result["retention_days"] == 30
    assert result["deleted_rows"] == 7
    assert len(executed_sql) == 1
    assert executed_sql[0][1] == (
        datetime(2026, 3, 7, 0, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None),
    )


def test_cleanup_mart_coin_daily_metrics_uses_date_cutoff(monkeypatch) -> None:
    executed_sql: list[tuple[str, tuple | None]] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "mart"
        yield _FakeConnection(executed_sql, rowcount=3)

    monkeypatch.setattr(mart_retention, "mysql_connection", fake_mysql_connection)
    monkeypatch.setenv("MART_RETENTION_ENABLED", "1")
    monkeypatch.setenv("MART_COIN_DAILY_RETENTION_DAYS", "365")

    result = mart_retention.cleanup_mart_coin_daily_metrics(
        now_utc=datetime(2026, 4, 6, 0, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    )

    assert result["retention_enabled"] is True
    assert result["retention_days"] == 365
    assert result["deleted_rows"] == 3
    assert len(executed_sql) == 1
    assert executed_sql[0][1] == (datetime(2025, 4, 6, 0, 0, 0).date(),)
