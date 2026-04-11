from __future__ import annotations

from src.load.load_raw_tables import _parse_epoch_to_utc_naive


def test_parse_epoch_to_utc_naive_preserves_millisecond_precision() -> None:
    parsed = _parse_epoch_to_utc_naive(1712419200123)

    assert parsed is not None
    assert parsed.year == 2024
    assert parsed.month == 4
    assert parsed.day == 6
    assert parsed.microsecond == 123000


def test_parse_epoch_to_utc_naive_supports_second_epoch() -> None:
    parsed = _parse_epoch_to_utc_naive(1712419200)

    assert parsed is not None
    assert parsed.year == 2024
    assert parsed.month == 4
    assert parsed.day == 6
    assert parsed.microsecond == 0


def test_parse_epoch_to_utc_naive_does_not_collapse_different_ms_points() -> None:
    ts_a = _parse_epoch_to_utc_naive(1712419200123)
    ts_b = _parse_epoch_to_utc_naive(1712419200789)

    assert ts_a is not None
    assert ts_b is not None
    assert ts_a != ts_b
