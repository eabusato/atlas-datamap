"""Integration tests for Phase 2B SQL Server connector support."""

from __future__ import annotations

import pytest

from atlas.connectors.mssql import TABLESAMPLE_THRESHOLD, MSSQLConnector

pytestmark = [pytest.mark.integration, pytest.mark.phase_2b]


def test_mssql_connect_and_ping(mssql_connector: MSSQLConnector) -> None:
    assert mssql_connector.ping() is True
    assert mssql_connector.server_version_info[0] >= 16


def test_mssql_get_schemas_excludes_system_schemas(mssql_connector: MSSQLConnector) -> None:
    schema_names = {schema.name for schema in mssql_connector.get_schemas()}
    assert "atlas_test" in schema_names
    assert "sys" not in schema_names


def test_mssql_get_tables_returns_synonyms_and_views(mssql_connector: MSSQLConnector) -> None:
    table_map = {table.name: table for table in mssql_connector.get_tables("atlas_test")}
    assert table_map["customers"].table_type.value == "table"
    assert table_map["v_active_customers"].table_type.value == "view"
    assert table_map["customer_alias"].table_type.value == "synonym"
    assert table_map["customers"].comment == "Customer registry"


def test_mssql_get_columns_and_foreign_keys(mssql_connector: MSSQLConnector) -> None:
    columns = {column.name: column for column in mssql_connector.get_columns("atlas_test", "orders")}
    foreign_keys = mssql_connector.get_foreign_keys("atlas_test", "orders")
    assert columns["id"].is_primary_key is True
    assert columns["id"].is_auto_increment is True
    assert columns["status"].default_value is not None
    assert foreign_keys[0].source_columns == ["customer_id"]
    assert foreign_keys[0].target_table == "customers"


def test_mssql_get_indexes_and_size_estimates(mssql_connector: MSSQLConnector) -> None:
    indexes = {index.name: index for index in mssql_connector.get_indexes("atlas_test", "orders")}
    assert "idx_customer" in indexes or "idx_customer_status" in indexes
    assert mssql_connector.get_row_count_estimate("atlas_test", "large_events") >= TABLESAMPLE_THRESHOLD
    assert mssql_connector.get_table_size_bytes("atlas_test", "customers") > 0


def test_mssql_sampling_masks_sensitive_values(mssql_connector_masked: MSSQLConnector) -> None:
    rows = mssql_connector_masked.get_sample_rows("atlas_test", "customers", limit=2)
    assert rows
    assert all(row["email"] == "***" for row in rows)
    assert all(row["name"] != "***" for row in rows)
