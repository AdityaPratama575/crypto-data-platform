from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from src.observability import ops_writer


class _FakeCursor:
    def __init__(self, executed: list[tuple[str, tuple | None]]) -> None:
        self._executed = executed

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:  # noqa: ANN001
        return False

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self._executed.append((sql, params))


class _FakeConnection:
    def __init__(self, executed: list[tuple[str, tuple | None]]) -> None:
        self._executed = executed

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._executed)


def test_ops_writer_writes_pipeline_and_task_queries(monkeypatch) -> None:
    executed: list[tuple[str, tuple | None]] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "ops"
        yield _FakeConnection(executed)

    monkeypatch.setattr(ops_writer, "mysql_connection", fake_mysql_connection)

    logical_ts = datetime(2026, 4, 5, 9, 0, 0, tzinfo=timezone.utc)
    started_ts = datetime(2026, 4, 5, 9, 0, 1, tzinfo=timezone.utc)
    ended_ts = datetime(2026, 4, 5, 9, 0, 2, tzinfo=timezone.utc)

    ops_writer.upsert_pipeline_run_started(
        dag_id="demo_dag",
        dag_run_id="demo_run",
        logical_run_ts_utc=logical_ts,
        started_at_utc=started_ts,
        message="started",
    )
    ops_writer.insert_task_run(
        dag_id="demo_dag",
        dag_run_id="demo_run",
        task_id="demo_task",
        started_at_utc=started_ts,
        ended_at_utc=ended_ts,
        task_status="success",
        rows_extracted=10,
        rows_loaded=10,
        rows_merged=9,
        error_message=None,
    )
    ops_writer.mark_pipeline_run_success(
        dag_id="demo_dag",
        dag_run_id="demo_run",
        ended_at_utc=ended_ts,
        message="done",
    )

    assert len(executed) == 3
    upsert_sql, upsert_params = executed[0]
    assert "INSERT INTO pipeline_run" in upsert_sql
    assert upsert_params is not None
    assert upsert_params[2].tzinfo is None
    assert upsert_params[3].tzinfo is None

    task_sql, task_params = executed[1]
    assert "INSERT INTO task_run" in task_sql
    assert task_params is not None
    assert task_params[3].tzinfo is None
    assert task_params[4].tzinfo is None

    success_sql, success_params = executed[2]
    assert "UPDATE pipeline_run" in success_sql
    assert success_params is not None
    assert success_params[0].tzinfo is None


def test_insert_dq_check_result_serializes_detail(monkeypatch) -> None:
    executed: list[tuple[str, tuple | None]] = []

    @contextmanager
    def fake_mysql_connection(*, layer: str):  # noqa: ANN202
        assert layer == "ops"
        yield _FakeConnection(executed)

    monkeypatch.setattr(ops_writer, "mysql_connection", fake_mysql_connection)

    ops_writer.insert_dq_check_result(
        dag_id="demo_dag",
        dag_run_id="demo_run",
        task_id="demo_task",
        check_name="row_count_positive",
        target_table="raw_coin_list_snapshot",
        check_status="pass",
        failed_row_count=0,
        checked_at_utc=datetime(2026, 4, 5, 9, 0, 3, tzinfo=timezone.utc),
        detail={"batch_id": "b1"},
    )

    assert len(executed) == 1
    sql, params = executed[0]
    assert "INSERT INTO dq_check_result" in sql
    assert params is not None
    assert params[7].tzinfo is None
    assert params[8] == '{"batch_id": "b1"}'
