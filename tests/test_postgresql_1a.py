"""Unit tests for Phase 1A PostgreSQL connection lifecycle and schema discovery."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine
from atlas.connectors.base import ConnectionError, QueryError
from atlas.connectors.postgresql import PostgreSQLConnector


def _make_config(**overrides: Any) -> AtlasConnectionConfig:
    payload = {
        "engine": DatabaseEngine.postgresql,
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
    connection.set_session = MagicMock()
    connection.rollback = MagicMock()

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
    pool.getconn = MagicMock(return_value=connection)
    pool.putconn = MagicMock()
    pool.closeall = MagicMock()
    return pool


class TestDsnBuilder:
    def test_basic_dsn_parts(self) -> None:
        connector = PostgreSQLConnector(_make_config())
        dsn = connector._build_dsn()
        assert "host=localhost" in dsn
        assert "dbname=atlas_test" in dsn
        assert "user=atlas" in dsn
        assert "password=secret" in dsn

    def test_custom_port_is_included(self) -> None:
        connector = PostgreSQLConnector(_make_config(port=5433))
        assert "port=5433" in connector._build_dsn()

    def test_ssl_verify_full_includes_cert_files(self) -> None:
        connector = PostgreSQLConnector(
            _make_config(
                ssl_mode="verify-full",
                connect_args={
                    "sslcert": "/certs/client.crt",
                    "sslkey": "/certs/client.key",
                    "sslrootcert": "/certs/ca.crt",
                },
            )
        )
        dsn = connector._build_dsn()
        assert "sslmode=verify-full" in dsn
        assert "sslcert=/certs/client.crt" in dsn
        assert "sslkey=/certs/client.key" in dsn
        assert "sslrootcert=/certs/ca.crt" in dsn

    def test_preferred_ssl_maps_to_prefer(self) -> None:
        connector = PostgreSQLConnector(_make_config(ssl_mode="preferred"))
        assert "sslmode=prefer" in connector._build_dsn()

    def test_application_name_can_be_overridden(self) -> None:
        connector = PostgreSQLConnector(
            _make_config(connect_args={"application_name": "atlas-tests"})
        )
        assert "application_name=atlas-tests" in connector._build_dsn()


class TestConnectLifecycle:
    @patch("atlas.connectors.postgresql._require_psycopg2")
    def test_connect_sets_connected_and_detects_version(self, require_psycopg2: MagicMock) -> None:
        psycopg2_mock = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchone = MagicMock(return_value=("PostgreSQL 15.4 on x86_64", "150004"))
        pool = _make_pool_with_cursor(cursor)
        psycopg2_mock.pool.ThreadedConnectionPool.return_value = pool
        require_psycopg2.return_value = psycopg2_mock

        connector = PostgreSQLConnector(_make_config())
        connector.connect()

        assert connector.is_connected
        assert connector.get_server_version() == "PostgreSQL 15.4"
        assert connector.server_version_info == (15, 0, 4)

    @patch("atlas.connectors.postgresql._require_psycopg2")
    def test_disconnect_closes_pool(self, require_psycopg2: MagicMock) -> None:
        psycopg2_mock = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchone = MagicMock(return_value=("PostgreSQL 15.4 on x86_64", "150004"))
        pool = _make_pool_with_cursor(cursor)
        psycopg2_mock.pool.ThreadedConnectionPool.return_value = pool
        require_psycopg2.return_value = psycopg2_mock

        connector = PostgreSQLConnector(_make_config())
        connector.connect()
        connector.disconnect()

        assert not connector.is_connected
        pool.closeall.assert_called_once()

    @patch("atlas.connectors.postgresql._require_psycopg2")
    def test_ping_returns_true_when_query_succeeds(self, require_psycopg2: MagicMock) -> None:
        psycopg2_mock = MagicMock()
        version_cursor = MagicMock()
        version_cursor.__enter__ = MagicMock(return_value=version_cursor)
        version_cursor.__exit__ = MagicMock(return_value=False)
        version_cursor.fetchone = MagicMock(return_value=("PostgreSQL 15.4 on x86_64", "150004"))

        ping_cursor = MagicMock()
        ping_cursor.__enter__ = MagicMock(return_value=ping_cursor)
        ping_cursor.__exit__ = MagicMock(return_value=False)
        ping_cursor.fetchone = MagicMock(return_value=(1,))

        pool = MagicMock()
        connection = MagicMock()
        connection.set_session = MagicMock()
        connection.rollback = MagicMock()
        setup_cursor = MagicMock()
        setup_cursor.__enter__ = MagicMock(return_value=setup_cursor)
        setup_cursor.__exit__ = MagicMock(return_value=False)
        cursor_sequence = [setup_cursor, version_cursor, setup_cursor, ping_cursor]

        def cursor_factory():
            return cursor_sequence.pop(0)

        connection.cursor = cursor_factory
        pool.getconn = MagicMock(return_value=connection)
        pool.putconn = MagicMock()
        pool.closeall = MagicMock()
        psycopg2_mock.pool.ThreadedConnectionPool.return_value = pool
        require_psycopg2.return_value = psycopg2_mock

        connector = PostgreSQLConnector(_make_config())
        connector.connect()
        assert connector.ping() is True

    def test_ping_returns_false_when_not_connected(self) -> None:
        assert PostgreSQLConnector(_make_config()).ping() is False

    @patch("atlas.connectors.postgresql._require_psycopg2")
    def test_connect_raises_connection_error_on_pool_failure(
        self, require_psycopg2: MagicMock
    ) -> None:
        psycopg2_mock = MagicMock()
        psycopg2_mock.pool.ThreadedConnectionPool.side_effect = RuntimeError("refused")
        require_psycopg2.return_value = psycopg2_mock

        connector = PostgreSQLConnector(_make_config())
        with pytest.raises(ConnectionError, match="Failed to connect"):
            connector.connect()


class TestSchemasAndStubs:
    def _connected_connector_with_fetchall(self, rows: list[tuple[str]]) -> PostgreSQLConnector:
        connector = PostgreSQLConnector(_make_config())
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchall = MagicMock(return_value=rows)
        connector._pool = _make_pool_with_cursor(cursor)
        connector._connected = True
        return connector

    def test_get_schemas_applies_filters(self) -> None:
        connector = self._connected_connector_with_fetchall(
            [("public",), ("pg_catalog",), ("analytics",), ("information_schema",)]
        )
        connector._config.schema_filter = ["public", "analytics"]
        connector._config.schema_exclude = ["analytics"]
        schemas = connector.get_schemas()
        assert [schema.name for schema in schemas] == ["public"]

    def test_get_schemas_returns_schema_info(self) -> None:
        connector = self._connected_connector_with_fetchall([("public",), ("billing",)])
        schemas = connector.get_schemas()
        assert [schema.name for schema in schemas] == ["public", "billing"]
        assert all(schema.engine == "postgresql" for schema in schemas)

    def test_cursor_raises_connection_error_when_disconnected(self) -> None:
        connector = PostgreSQLConnector(_make_config())
        with pytest.raises(ConnectionError, match="not connected"), connector._cursor():
            pass

    def test_cursor_wraps_query_failures(self) -> None:
        connector = PostgreSQLConnector(_make_config())
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.execute.side_effect = RuntimeError("boom")
        connector._pool = _make_pool_with_cursor(cursor)
        connector._connected = True
        with pytest.raises(QueryError, match="PostgreSQL query failed"), connector._cursor() as cur:
            cur.execute("SELECT 1")

    def test_repr_includes_version_when_connected(self) -> None:
        connector = PostgreSQLConnector(_make_config())
        connector._connected = True
        connector._server_version = "PostgreSQL 15.4"
        assert "PostgreSQL 15.4" in repr(connector)
