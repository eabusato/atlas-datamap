"""Integration tests for Phase 2C canonical type propagation."""

from __future__ import annotations

import pytest

from atlas.connectors.mssql import MSSQLConnector
from atlas.connectors.mysql import MySQLConnector
from atlas.connectors.postgresql import PostgreSQLConnector
from atlas.types import AtlasType

pytestmark = [pytest.mark.integration, pytest.mark.phase_2c]


def test_postgresql_columns_populate_canonical_types(pg_connector: PostgreSQLConnector) -> None:
    columns = {column.name: column for column in pg_connector.get_columns("atlas_test", "customers")}
    assert columns["id"].canonical_type is AtlasType.INTEGER
    assert columns["email"].canonical_type is AtlasType.TEXT
    assert columns["active"].canonical_type is AtlasType.BOOLEAN
    assert columns["created_at"].canonical_type is AtlasType.DATETIME


def test_postgresql_json_and_clob_types_are_normalized(pg_connector: PostgreSQLConnector) -> None:
    product_columns = {
        column.name: column for column in pg_connector.get_columns("atlas_test", "products")
    }
    order_columns = {column.name: column for column in pg_connector.get_columns("atlas_test", "orders")}
    assert product_columns["metadata"].canonical_type is AtlasType.JSON
    assert order_columns["notes"].canonical_type is AtlasType.CLOB


def test_mysql_columns_populate_canonical_types(mysql_connector: MySQLConnector) -> None:
    columns = {column.name: column for column in mysql_connector.get_columns("atlas_test", "customers")}
    assert columns["id"].canonical_type is AtlasType.INTEGER
    assert columns["email"].canonical_type is AtlasType.TEXT
    assert columns["phone"].canonical_type is AtlasType.TEXT
    assert columns["created_at"].canonical_type is AtlasType.DATETIME


def test_mysql_boolean_json_and_decimal_types_are_normalized(
    mysql_connector: MySQLConnector,
) -> None:
    columns = {column.name: column for column in mysql_connector.get_columns("atlas_test", "products")}
    assert columns["is_active"].canonical_type is AtlasType.BOOLEAN
    assert columns["metadata"].canonical_type is AtlasType.JSON
    assert columns["price"].canonical_type is AtlasType.DECIMAL


def test_mariadb_uses_mysql_type_normalization(mariadb_connector: MySQLConnector) -> None:
    columns = {column.name: column for column in mariadb_connector.get_columns("atlas_test", "products")}
    assert columns["is_active"].canonical_type is AtlasType.BOOLEAN
    assert columns["metadata"].canonical_type is AtlasType.JSON
    assert columns["price"].canonical_type is AtlasType.DECIMAL


def test_mssql_columns_populate_canonical_types(mssql_connector: MSSQLConnector) -> None:
    customer_columns = {
        column.name: column for column in mssql_connector.get_columns("atlas_test", "customers")
    }
    product_columns = {
        column.name: column for column in mssql_connector.get_columns("atlas_test", "products")
    }
    assert customer_columns["id"].canonical_type is AtlasType.INTEGER
    assert customer_columns["email"].canonical_type is AtlasType.TEXT
    assert customer_columns["created_at"].canonical_type is AtlasType.DATETIME
    assert product_columns["is_active"].canonical_type is AtlasType.BOOLEAN
    assert product_columns["metadata"].canonical_type is AtlasType.TEXT
