"""Unit tests for Phase 2A MySQL and MariaDB connector behavior."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors.base import ConnectionError, QueryError
from atlas.connectors.mysql import RAND_THRESHOLD, MySQLConnector
from atlas.types import AtlasType


def _make_config(**overrides: Any) -> AtlasConnectionConfig:
    payload = {
        "engine": DatabaseEngine.mysql,
        "host": "localhost",
        "database": "atlas_test",
        "user": "atlas",
        "password": "secret",
    }
    payload.update(overrides)
    return AtlasConnectionConfig(**payload)


def _make_pool_with_cursor(cursor: MagicMock) -> MagicMock:
    pool = MagicMock()
    connection = MagicMock()
    setup_cursor = MagicMock()
    setup_cursor.__enter__ = MagicMock(return_value=setup_cursor)
    setup_cursor.__exit__ = MagicMock(return_value=False)

    call_counter = {"count": 0}

    def cursor_factory():
        call_counter["count"] += 1
        if call_counter["count"] % 2 == 1:
            return setup_cursor
        return cursor

    connection.cursor = cursor_factory
    connection.rollback = MagicMock()
    connection.close = MagicMock()
    connection.start_transaction = MagicMock()
    pool.get_connection = MagicMock(return_value=connection)
    return pool


def _install_cursor_manager(
    connector: MySQLConnector,
    rows: list[tuple[Any, ...]],
    columns: list[str],
) -> MagicMock:
    from contextlib import contextmanager

    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.description = [(column,) for column in columns]
    cursor.fetchall = MagicMock(return_value=rows)

    @contextmanager
    def cursor_manager():
        yield cursor

    connector._cursor = cursor_manager  # type: ignore[method-assign]
    return cursor


class TestConnectLifecycle:
    @patch("atlas.connectors.mysql._require_mysql_connector")
    def test_connect_detects_mysql_version(self, require_mysql: MagicMock) -> None:
        mysql_module = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchone = MagicMock(return_value=("8.0.36",))
        pool = _make_pool_with_cursor(cursor)
        mysql_module.pooling.MySQLConnectionPool.return_value = pool
        require_mysql.return_value = mysql_module

        connector = MySQLConnector(_make_config())
        connector.connect()

        assert connector.is_connected
        assert connector.get_server_version() == "8.0.36"
        assert connector.server_version_info == (8, 0, 36)
        assert connector.is_mariadb is False

    @patch("atlas.connectors.mysql._require_mysql_connector")
    def test_connect_detects_mariadb_variant(self, require_mysql: MagicMock) -> None:
        mysql_module = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchone = MagicMock(return_value=("10.11.6-MariaDB-1:10.11.6+maria~ubu2204",))
        pool = _make_pool_with_cursor(cursor)
        mysql_module.pooling.MySQLConnectionPool.return_value = pool
        require_mysql.return_value = mysql_module

        connector = MySQLConnector(_make_config())
        connector.connect()

        assert connector.is_mariadb is True
        assert connector.server_version_info == (10, 11, 6)

    @patch("atlas.connectors.mysql._require_mysql_connector")
    def test_connect_raises_connection_error_on_pool_failure(self, require_mysql: MagicMock) -> None:
        mysql_module = MagicMock()
        mysql_module.pooling.MySQLConnectionPool.side_effect = RuntimeError("refused")
        require_mysql.return_value = mysql_module

        connector = MySQLConnector(_make_config())
        with pytest.raises(ConnectionError, match="Failed to connect"):
            connector.connect()

    def test_disconnect_clears_pool_and_state(self) -> None:
        connector = MySQLConnector(_make_config())
        connector._pool = MagicMock()
        connector._connected = True
        connector.disconnect()
        assert connector._pool is None
        assert connector.is_connected is False

    def test_ping_returns_false_when_not_connected(self) -> None:
        assert MySQLConnector(_make_config()).ping() is False


class TestSchemasAndTables:
    def test_get_schemas_filters_system_and_excluded_schemas(self) -> None:
        connector = MySQLConnector(_make_config())
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchall = MagicMock(
            return_value=[("atlas_test",), ("mysql",), ("analytics",), ("sys",)]
        )
        connector._pool = _make_pool_with_cursor(cursor)
        connector._connected = True
        connector._config.schema_exclude = ["analytics"]

        schemas = connector.get_schemas()

        assert [schema.name for schema in schemas] == ["atlas_test"]

    def test_get_tables_returns_base_tables_and_views(self) -> None:
        connector = MySQLConnector(_make_config())
        connector._fetchall = MagicMock(  # type: ignore[method-assign]
            return_value=[
                ("customers", "BASE TABLE", "Customer registry", 10, 4096),
                ("v_active_customers", "VIEW", None, 0, 0),
            ]
        )

        tables = connector.get_tables("atlas_test")

        assert [table.name for table in tables] == ["customers", "v_active_customers"]
        assert tables[0].table_type.value == "table"
        assert tables[1].table_type.value == "view"

    def test_get_row_count_estimate_returns_zero_when_missing(self) -> None:
        connector = MySQLConnector(_make_config())
        connector._fetchone = MagicMock(return_value=None)  # type: ignore[method-assign]
        assert connector.get_row_count_estimate("atlas_test", "missing") == 0

    def test_get_table_size_bytes_returns_integer(self) -> None:
        connector = MySQLConnector(_make_config())
        connector._fetchone = MagicMock(return_value=(8192,))  # type: ignore[method-assign]
        assert connector.get_table_size_bytes("atlas_test", "customers") == 8192


class TestColumnsRelationshipsAndIndexes:
    def test_get_columns_maps_defaults_comments_and_auto_increment(self) -> None:
        connector = MySQLConnector(_make_config())
        connector._fetchall = MagicMock(  # type: ignore[method-assign]
            return_value=[
                (
                    "id",
                    "int unsigned",
                    "int",
                    "NO",
                    None,
                    1,
                    "PRI",
                    None,
                    "auto_increment",
                    None,
                ),
                (
                    "email",
                    "varchar(255)",
                    "varchar",
                    "NO",
                    None,
                    2,
                    "UNI",
                    "Email address",
                    "",
                    None,
                ),
            ]
        )

        columns = connector.get_columns("atlas_test", "customers")

        assert columns[0].is_primary_key is True
        assert columns[0].is_auto_increment is True
        assert columns[0].canonical_type is AtlasType.INTEGER
        assert columns[1].is_unique is True
        assert columns[1].canonical_type is AtlasType.TEXT
        assert columns[1].comment == "Email address"

    def test_get_columns_detects_mariadb_json_alias(self) -> None:
        connector = MySQLConnector(_make_config())
        connector._is_mariadb = True
        connector._fetchall = MagicMock(  # type: ignore[method-assign]
            return_value=[
                (
                    "metadata",
                    "longtext",
                    "longtext",
                    "YES",
                    None,
                    8,
                    "",
                    None,
                    "",
                    "json_valid(`metadata`)",
                ),
            ]
        )

        columns = connector.get_columns("atlas_test", "products")

        assert columns[0].native_type == "json"
        assert columns[0].canonical_type is AtlasType.JSON

    def test_get_foreign_keys_groups_composite_rows(self) -> None:
        connector = MySQLConnector(_make_config())
        connector._fetchall = MagicMock(  # type: ignore[method-assign]
            return_value=[
                ("fk_order_items_order", "order_id", "atlas_test", "orders", "id", "CASCADE", "CASCADE"),
                ("fk_order_items_order", "tenant_id", "atlas_test", "orders", "tenant_id", "CASCADE", "CASCADE"),
            ]
        )

        foreign_keys = connector.get_foreign_keys("atlas_test", "order_items")

        assert len(foreign_keys) == 1
        assert foreign_keys[0].source_columns == ["order_id", "tenant_id"]
        assert foreign_keys[0].target_columns == ["id", "tenant_id"]

    def test_get_indexes_groups_columns_and_detects_partial_prefix(self) -> None:
        connector = MySQLConnector(_make_config())
        connector._fetchall = MagicMock(  # type: ignore[method-assign]
            return_value=[
                ("PRIMARY", "id", 0, "BTREE", 1, None),
                ("idx_email_prefix", "email", 1, "BTREE", 1, 12),
            ]
        )

        indexes = connector.get_indexes("atlas_test", "customers")

        assert indexes[0].is_primary is True
        assert indexes[1].is_partial is True
        assert indexes[1].columns == ["email"]

    def test_distinct_estimate_uses_statistics_cardinality(self) -> None:
        connector = MySQLConnector(_make_config())
        connector._fetchone = MagicMock(return_value=(55,))  # type: ignore[method-assign]
        assert connector.get_column_distinct_estimate("atlas_test", "customers", "status") == 55


class TestSampling:
    def test_small_table_uses_rand_order(self) -> None:
        connector = MySQLConnector(_make_config())
        connector.get_row_count_estimate = MagicMock(return_value=RAND_THRESHOLD - 1)
        cursor = _install_cursor_manager(connector, [(1, "Alice")], ["id", "name"])

        rows = connector.get_sample_rows("atlas_test", "customers", limit=10)

        assert rows == [{"id": "1", "name": "Alice"}]
        sql, params = cursor.execute.call_args[0]
        assert "ORDER BY RAND()" in sql
        assert params == (10,)

    @patch("atlas.connectors.mysql.random.randint", return_value=7)
    def test_large_table_uses_limit_with_offset(self, randint: MagicMock) -> None:
        connector = MySQLConnector(_make_config())
        connector.get_row_count_estimate = MagicMock(return_value=RAND_THRESHOLD + 100)
        cursor = _install_cursor_manager(connector, [(1,)], ["id"])

        connector.get_sample_rows("atlas_test", "events", limit=5)

        sql, params = cursor.execute.call_args[0]
        assert "OFFSET %s" in sql
        assert params == (5, 7)
        randint.assert_called_once()

    def test_masked_mode_hides_sensitive_values(self) -> None:
        connector = MySQLConnector(_make_config(privacy_mode=PrivacyMode.masked))
        connector.get_row_count_estimate = MagicMock(return_value=10)
        _install_cursor_manager(
            connector,
            [("alice@example.com", "Alice")],
            ["email", "name"],
        )

        rows = connector.get_sample_rows("atlas_test", "customers", limit=1)

        assert rows == [{"email": "***", "name": "Alice"}]

    def test_cursor_raises_connection_error_when_disconnected(self) -> None:
        connector = MySQLConnector(_make_config())
        with pytest.raises(ConnectionError, match="not connected"), connector._cursor():
            pass

    def test_cursor_wraps_query_failures(self) -> None:
        connector = MySQLConnector(_make_config())
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.execute.side_effect = RuntimeError("boom")
        connector._pool = _make_pool_with_cursor(cursor)
        connector._connected = True

        with pytest.raises(QueryError, match="MySQL query failed"), connector._cursor() as cur:
            cur.execute("SELECT 1")
