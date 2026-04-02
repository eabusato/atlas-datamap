"""Unit tests for Phase 2B SQL Server connector behavior."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors.base import ConnectionError, QueryError
from atlas.connectors.mssql import (
    TABLESAMPLE_THRESHOLD,
    MSSQLConnector,
    _compose_mssql_native_type,
)
from atlas.types import AtlasType


def _make_config(**overrides: Any) -> AtlasConnectionConfig:
    payload = {
        "engine": DatabaseEngine.mssql,
        "host": "localhost",
        "port": 1433,
        "database": "atlas_test",
        "user": "sa",
        "password": "Secret!123",
    }
    payload.update(overrides)
    return AtlasConnectionConfig(**payload)


def _install_cursor_manager(
    connector: MSSQLConnector,
    rows: list[tuple[Any, ...]],
    columns: list[str] | None = None,
) -> MagicMock:
    from contextlib import contextmanager

    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchall = MagicMock(return_value=rows)
    cursor.fetchone = MagicMock(return_value=rows[0] if rows else None)
    cursor.description = None if columns is None else [(column,) for column in columns]

    @contextmanager
    def cursor_manager():
        yield cursor

    connector._cursor = cursor_manager  # type: ignore[method-assign]
    return cursor


class TestNativeTypeFormatting:
    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (("nvarchar", 40, None, None), "nvarchar(20)"),
            (("varchar", 255, None, None), "varchar(255)"),
            (("decimal", None, 10, 2), "decimal(10,2)"),
            (("varbinary", -1, None, None), "varbinary(max)"),
            (("datetime2", None, None, None), "datetime2"),
        ],
    )
    def test_compose_mssql_native_type(self, args: tuple[Any, ...], expected: str) -> None:
        assert _compose_mssql_native_type(*args) == expected


class TestConnectionLifecycle:
    @patch("atlas.connectors.mssql._MSSQLConnectionPool")
    def test_connect_sets_connected_and_detects_version(self, pool_class: MagicMock) -> None:
        pool = MagicMock()
        pool.get_connection.return_value = MagicMock()
        pool_class.return_value = pool
        connector = MSSQLConnector(_make_config())
        connector._detect_server_version = MagicMock(  # type: ignore[method-assign]
            return_value=("Microsoft SQL Server 2022 - 16.0.1000.6", (16, 0, 1000), "Developer")
        )

        connector.connect()

        assert connector.is_connected is True
        assert connector.server_version_info == (16, 0, 1000)
        assert connector.edition == "Developer"

    @patch("atlas.connectors.mssql._MSSQLConnectionPool")
    def test_connect_raises_connection_error(self, pool_class: MagicMock) -> None:
        pool_class.side_effect = RuntimeError("boom")
        connector = MSSQLConnector(_make_config())
        with pytest.raises(ConnectionError, match="Failed to connect"):
            connector.connect()

    def test_disconnect_closes_pool(self) -> None:
        connector = MSSQLConnector(_make_config())
        connector._pool = MagicMock()
        connector._connected = True
        connector.disconnect()
        assert connector.is_connected is False
        connector._pool = None

    def test_ping_returns_false_when_not_connected(self) -> None:
        assert MSSQLConnector(_make_config()).ping() is False


class TestMetadataQueries:
    def test_get_schemas_filters_system_entries(self) -> None:
        connector = MSSQLConnector(_make_config())
        connector._fetchall = MagicMock(return_value=[("atlas_test",), ("sys",), ("sales",)])  # type: ignore[method-assign]
        connector._config.schema_exclude = ["sales"]

        schemas = connector.get_schemas()

        assert [schema.name for schema in schemas] == ["atlas_test"]

    def test_get_tables_maps_view_and_synonym_types(self) -> None:
        connector = MSSQLConnector(_make_config())
        connector._fetchall = MagicMock(  # type: ignore[method-assign]
            return_value=[
                ("customers", "U", "Customers"),
                ("v_active_customers", " v ", None),
                ("customer_alias", "sn", None),
            ]
        )

        tables = connector.get_tables("atlas_test")

        assert [table.table_type.value for table in tables] == ["table", "view", "synonym"]

    def test_get_columns_returns_identity_default_and_comment(self) -> None:
        connector = MSSQLConnector(_make_config())
        connector._fetchall = MagicMock(  # type: ignore[method-assign]
            return_value=[
                ("id", "int", None, None, None, False, True, "(1)", 1, "Primary key", 1),
                ("email", "nvarchar", 510, None, None, False, False, None, None, "Email", 2),
            ]
        )

        columns = connector.get_columns("atlas_test", "customers")

        assert columns[0].is_primary_key is True
        assert columns[0].is_auto_increment is True
        assert columns[0].canonical_type is AtlasType.INTEGER
        assert columns[1].canonical_type is AtlasType.TEXT
        assert columns[1].native_type == "nvarchar(255)"

    def test_get_foreign_keys_groups_rows(self) -> None:
        connector = MSSQLConnector(_make_config())
        connector._fetchall = MagicMock(  # type: ignore[method-assign]
            return_value=[
                ("fk_orders_customer", 1, "customer_id", "atlas_test", "customers", "id", "NO_ACTION", "CASCADE"),
                ("fk_orders_customer", 2, "tenant_id", "atlas_test", "customers", "tenant_id", "NO_ACTION", "CASCADE"),
            ]
        )

        foreign_keys = connector.get_foreign_keys("atlas_test", "orders")

        assert len(foreign_keys) == 1
        assert foreign_keys[0].source_columns == ["customer_id", "tenant_id"]

    def test_get_indexes_groups_columns(self) -> None:
        connector = MSSQLConnector(_make_config())
        connector._fetchall = MagicMock(  # type: ignore[method-assign]
            return_value=[
                ("pk_customers", "CLUSTERED", False, True, False, "id", 1),
                ("idx_customer_status", "NONCLUSTERED", False, False, True, "customer_id", 1),
                ("idx_customer_status", "NONCLUSTERED", False, False, True, "status", 2),
            ]
        )

        indexes = connector.get_indexes("atlas_test", "orders")

        assert indexes[0].is_primary is True
        assert indexes[1].columns == ["customer_id", "status"]
        assert indexes[1].is_partial is True

    def test_row_count_and_size_defaults_to_zero(self) -> None:
        connector = MSSQLConnector(_make_config())
        connector._fetchone = MagicMock(return_value=None)  # type: ignore[method-assign]
        assert connector.get_row_count_estimate("atlas_test", "missing") == 0
        assert connector.get_table_size_bytes("atlas_test", "missing") == 0


class TestSampling:
    def test_small_table_uses_top_order_by_newid(self) -> None:
        connector = MSSQLConnector(_make_config())
        connector.get_row_count_estimate = MagicMock(return_value=TABLESAMPLE_THRESHOLD - 1)
        cursor = _install_cursor_manager(connector, [(1, "Alice")], ["id", "name"])

        rows = connector.get_sample_rows("atlas_test", "customers", limit=5)

        assert rows == [{"id": "1", "name": "Alice"}]
        cursor.execute.assert_called_once()
        assert "ORDER BY NEWID()" in cursor.execute.call_args[0][0]

    def test_large_table_uses_tablesample_and_fallback(self) -> None:
        connector = MSSQLConnector(_make_config())
        connector.get_row_count_estimate = MagicMock(return_value=TABLESAMPLE_THRESHOLD + 1)

        from contextlib import contextmanager

        first = MagicMock()
        first.description = [("id",)]
        first.fetchall = MagicMock(return_value=[])
        second = MagicMock()
        second.description = [("id",)]
        second.fetchall = MagicMock(return_value=[(42,)])
        queue = [first, second]

        @contextmanager
        def cursor_manager():
            yield queue.pop(0)

        connector._cursor = cursor_manager  # type: ignore[method-assign]

        rows = connector.get_sample_rows("atlas_test", "events", limit=10)

        assert rows == [{"id": "42"}]
        assert "TABLESAMPLE" in first.execute.call_args[0][0]
        assert "ORDER BY NEWID()" in second.execute.call_args[0][0]

    def test_masked_mode_hides_sensitive_values(self) -> None:
        connector = MSSQLConnector(_make_config(privacy_mode=PrivacyMode.masked))
        connector.get_row_count_estimate = MagicMock(return_value=100)
        _install_cursor_manager(connector, [("alice@example.com", "Alice")], ["email", "name"])

        rows = connector.get_sample_rows("atlas_test", "customers", limit=1)

        assert rows == [{"email": "***", "name": "Alice"}]

    def test_cursor_raises_connection_error_when_disconnected(self) -> None:
        connector = MSSQLConnector(_make_config())
        with pytest.raises(ConnectionError, match="not connected"), connector._cursor():
            pass

    def test_cursor_wraps_query_failures(self) -> None:
        connector = MSSQLConnector(_make_config())
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("boom")
        connection = MagicMock()
        connection.cursor.return_value = cursor
        connection.rollback = MagicMock()
        pool = MagicMock()
        pool.get_connection.return_value = connection
        pool.return_connection = MagicMock()
        connector._pool = pool
        connector._connected = True

        with pytest.raises(QueryError, match="SQL Server query failed"), connector._cursor() as cur:
            cur.execute("SELECT 1")
