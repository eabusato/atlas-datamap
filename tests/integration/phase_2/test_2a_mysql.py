"""Integration tests for Phase 2A MySQL and MariaDB connector support."""

from __future__ import annotations

import pytest

from atlas.connectors.mysql import RAND_THRESHOLD, MySQLConnector

pytestmark = [pytest.mark.integration, pytest.mark.phase_2a]


def test_mysql_connect_and_ping(mysql_connector: MySQLConnector) -> None:
    assert mysql_connector.ping() is True
    assert mysql_connector.is_mariadb is False


def test_mysql_get_schemas_excludes_system_schemas(mysql_connector: MySQLConnector) -> None:
    schema_names = {schema.name for schema in mysql_connector.get_schemas()}
    assert "atlas_test" in schema_names
    assert "mysql" not in schema_names
    assert "information_schema" not in schema_names


def test_mysql_get_tables_returns_tables_and_views(mysql_connector: MySQLConnector) -> None:
    tables = mysql_connector.get_tables("atlas_test")
    table_map = {table.name: table for table in tables}
    assert table_map["customers"].table_type.value == "table"
    assert table_map["v_active_customers"].table_type.value == "view"
    assert table_map["customers"].comment == "Customer registry"


def test_mysql_get_columns_and_foreign_keys(mysql_connector: MySQLConnector) -> None:
    columns = {column.name: column for column in mysql_connector.get_columns("atlas_test", "orders")}
    foreign_keys = mysql_connector.get_foreign_keys("atlas_test", "orders")
    assert columns["id"].is_primary_key is True
    assert columns["id"].is_auto_increment is True
    assert columns["status"].default_value == "pending"
    assert foreign_keys[0].source_columns == ["customer_id"]
    assert foreign_keys[0].target_table == "customers"


def test_mysql_get_indexes_and_size_estimates(mysql_connector: MySQLConnector) -> None:
    indexes = {index.name: index for index in mysql_connector.get_indexes("atlas_test", "orders")}
    assert "idx_customer" in indexes
    assert "idx_customer_status" in indexes
    assert mysql_connector.get_row_count_estimate("atlas_test", "large_events") >= RAND_THRESHOLD
    assert mysql_connector.get_table_size_bytes("atlas_test", "customers") > 0


def test_mysql_sampling_masks_sensitive_values(mysql_connector_masked: MySQLConnector) -> None:
    rows = mysql_connector_masked.get_sample_rows("atlas_test", "customers", limit=2)
    assert rows
    assert all(row["email"] == "***" for row in rows)
    assert all(row["name"] != "***" for row in rows)


def test_mariadb_connector_detects_variant(mariadb_connector: MySQLConnector) -> None:
    assert mariadb_connector.ping() is True
    assert mariadb_connector.is_mariadb is True
    assert mariadb_connector.server_version_info[0] >= 10
