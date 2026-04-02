"""Integration tests for Phase 1A PostgreSQL connection and schema discovery."""

from __future__ import annotations

import pytest

from atlas.config import AtlasConnectionConfig, PrivacyMode
from atlas.connectors.postgresql import PostgreSQLConnector

pytestmark = pytest.mark.integration


def test_connect_and_ping(pg_connector: PostgreSQLConnector) -> None:
    assert pg_connector.ping() is True


def test_detects_server_version(pg_connector: PostgreSQLConnector) -> None:
    assert pg_connector.get_server_version().startswith("PostgreSQL ")
    assert pg_connector.server_version_info[0] >= 15


def test_get_schemas_returns_atlas_test_schema(pg_connector: PostgreSQLConnector) -> None:
    schema_names = {schema.name for schema in pg_connector.get_schemas()}
    assert "atlas_test" in schema_names


def test_get_schemas_excludes_system_schemas(pg_connector: PostgreSQLConnector) -> None:
    schema_names = {schema.name for schema in pg_connector.get_schemas()}
    assert "pg_catalog" not in schema_names
    assert "information_schema" not in schema_names


def test_schema_filter_is_applied(pg_test_db) -> None:
    config = AtlasConnectionConfig.from_url(
        "postgresql://atlas_test:atlas_test@localhost:5433/atlas_test",
        privacy_mode=PrivacyMode.normal,
        schema_filter=["atlas_test"],
    )
    connector = PostgreSQLConnector(config)
    connector.connect()
    try:
        schema_names = [schema.name for schema in connector.get_schemas()]
    finally:
        connector.disconnect()
    assert schema_names == ["atlas_test"]


def test_disconnect_and_reconnect(pg_connector: PostgreSQLConnector) -> None:
    pg_connector.disconnect()
    assert pg_connector.is_connected is False
    pg_connector.connect()
    assert pg_connector.ping() is True
