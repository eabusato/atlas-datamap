"""Unit tests for Phase 1D PostgreSQL sampling and pg_stats-derived statistics."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors.base import PrivacyViolationError
from atlas.connectors.postgresql import (
    TABLESAMPLE_THRESHOLD,
    PostgreSQLConnector,
    _parse_pg_array,
)


def _make_config(**overrides: Any) -> AtlasConnectionConfig:
    payload = {
        "engine": DatabaseEngine.postgresql,
        "host": "localhost",
        "database": "atlas_test",
        "privacy_mode": PrivacyMode.normal,
    }
    payload.update(overrides)
    return AtlasConnectionConfig(**payload)


def _make_connector(**config_overrides: Any) -> PostgreSQLConnector:
    connector = PostgreSQLConnector(_make_config(**config_overrides))
    connector._connected = True
    return connector


def _install_cursor(connector: PostgreSQLConnector, rows: list[tuple[Any, ...]], columns: list[str]) -> MagicMock:
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.description = [(column,) for column in columns]
    cursor.fetchall = MagicMock(return_value=rows)

    @pytest.fixture
    def _unused() -> None:
        return None

    from contextlib import contextmanager

    @contextmanager
    def cursor_manager():
        yield cursor

    connector._cursor = cursor_manager  # type: ignore[method-assign]
    return cursor


def _install_cursor_sequence(
    connector: PostgreSQLConnector,
    payloads: list[tuple[list[tuple[Any, ...]], list[str]]],
) -> list[MagicMock]:
    from contextlib import contextmanager

    cursors: list[MagicMock] = []
    for rows, columns in payloads:
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.description = [(column,) for column in columns]
        cursor.fetchall = MagicMock(return_value=rows)
        cursors.append(cursor)

    queue = list(cursors)

    @contextmanager
    def cursor_manager():
        yield queue.pop(0)

    connector._cursor = cursor_manager  # type: ignore[method-assign]
    return cursors


class TestArrayParsing:
    def test_parse_pg_array_handles_quotes(self) -> None:
        assert _parse_pg_array('{"a","b"}') == ["a", "b"]

    def test_parse_pg_array_handles_empty_array(self) -> None:
        assert _parse_pg_array("{}") == []


class TestSampleRows:
    def test_small_table_uses_limit(self) -> None:
        connector = _make_connector()
        connector.get_row_count_estimate = MagicMock(return_value=TABLESAMPLE_THRESHOLD - 1)
        cursor = _install_cursor(connector, [(1, "Alice")], ["id", "name"])

        rows = connector.get_sample_rows("public", "users", limit=10)

        assert rows == [{"id": "1", "name": "Alice"}]
        sql, params = cursor.execute.call_args[0]
        assert "LIMIT %s" in sql
        assert "TABLESAMPLE" not in sql
        assert params == (10,)

    def test_large_table_uses_tablesample(self) -> None:
        connector = _make_connector()
        connector.get_row_count_estimate = MagicMock(return_value=TABLESAMPLE_THRESHOLD * 10)
        cursor = _install_cursor(connector, [(1,)], ["id"])

        connector.get_sample_rows("public", "events", limit=100)

        sql, params = cursor.execute.call_args[0]
        assert "TABLESAMPLE SYSTEM(%s)" in sql
        assert params[1] == 100

    def test_masked_mode_hides_sensitive_columns(self) -> None:
        connector = _make_connector(privacy_mode=PrivacyMode.masked)
        connector.get_row_count_estimate = MagicMock(return_value=100)
        cursor = _install_cursor(
            connector,
            [(1, "alice@example.com", "Alice")],
            ["id", "email", "display_name"],
        )

        rows = connector.get_sample_rows("public", "customers", limit=1)

        assert rows[0]["email"] == "***"
        assert rows[0]["display_name"] == "Alice"
        assert cursor.execute.called

    def test_stats_only_raises_before_query(self) -> None:
        connector = _make_connector(privacy_mode=PrivacyMode.stats_only)
        connector.get_row_count_estimate = MagicMock()
        with pytest.raises(PrivacyViolationError):
            connector.get_sample_rows("public", "customers")
        connector.get_row_count_estimate.assert_not_called()

    def test_no_samples_raises_before_query(self) -> None:
        connector = _make_connector(privacy_mode=PrivacyMode.no_samples)
        connector.get_row_count_estimate = MagicMock()
        with pytest.raises(PrivacyViolationError):
            connector.get_sample_rows("public", "customers")
        connector.get_row_count_estimate.assert_not_called()

    def test_large_table_falls_back_to_limit_when_tablesample_is_empty(self) -> None:
        connector = _make_connector()
        connector.get_row_count_estimate = MagicMock(return_value=TABLESAMPLE_THRESHOLD * 10)
        cursors = _install_cursor_sequence(
            connector,
            [
                ([], ["id"]),
                ([(42,)], ["id"]),
            ],
        )

        rows = connector.get_sample_rows("public", "events", limit=25)

        assert rows == [{"id": "42"}]
        first_sql, first_params = cursors[0].execute.call_args[0]
        second_sql, second_params = cursors[1].execute.call_args[0]
        assert "TABLESAMPLE SYSTEM(%s)" in first_sql
        assert first_params[1] == 25
        assert "TABLESAMPLE" not in second_sql
        assert second_params == (25,)

    def test_zero_limit_short_circuits(self) -> None:
        connector = _make_connector()
        connector.get_row_count_estimate = MagicMock()
        assert connector.get_sample_rows("public", "customers", limit=0) == []
        connector.get_row_count_estimate.assert_not_called()


class TestColumnStats:
    def test_get_column_stats_uses_pg_stats_row(self) -> None:
        connector = _make_connector()
        connector.get_row_count_estimate = MagicMock(return_value=1000)
        connector._fetch_pg_stats_row = MagicMock(return_value=(0.25, 100, "{1,10,100}"))  # type: ignore[method-assign]

        stats = connector.get_column_stats("public", "orders", "id")

        assert stats.row_count == 1000
        assert stats.null_count == 250
        assert stats.distinct_count == 100
        assert stats.min_value == "1"
        assert stats.max_value == "100"

    def test_negative_n_distinct_is_scaled_by_row_count(self) -> None:
        connector = _make_connector()
        connector.get_row_count_estimate = MagicMock(return_value=1000)
        connector._fetch_pg_stats_row = MagicMock(return_value=(0.0, -0.5, None))  # type: ignore[method-assign]
        assert connector.get_column_distinct_estimate("public", "orders", "customer_id") == 500

    def test_nearly_unique_negative_n_distinct_returns_row_count(self) -> None:
        connector = _make_connector()
        connector.get_row_count_estimate = MagicMock(return_value=1000)
        connector._fetch_pg_stats_row = MagicMock(return_value=(0.0, -1.0, None))  # type: ignore[method-assign]
        assert connector.get_column_distinct_estimate("public", "orders", "id") == 1000

    def test_missing_pg_stats_returns_zero_counts(self) -> None:
        connector = _make_connector()
        connector.get_row_count_estimate = MagicMock(return_value=1000)
        connector._fetch_pg_stats_row = MagicMock(return_value=None)  # type: ignore[method-assign]
        stats = connector.get_column_stats("public", "orders", "missing")
        assert stats.null_count == 0
        assert stats.distinct_count == 0
        assert stats.min_value == ""

    def test_get_column_null_count_uses_pg_stats(self) -> None:
        connector = _make_connector()
        connector.get_row_count_estimate = MagicMock(return_value=200)
        connector._fetch_pg_stats_row = MagicMock(return_value=(0.1, 50, None))  # type: ignore[method-assign]
        assert connector.get_column_null_count("public", "orders", "notes") == 20
