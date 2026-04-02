"""Integration tests for Phase 1B PostgreSQL tables, volumes, and columns."""

from __future__ import annotations

import pytest

from atlas.connectors.postgresql import PostgreSQLConnector
from atlas.types import TableType

pytestmark = pytest.mark.integration


def test_get_tables_returns_base_tables(pg_connector: PostgreSQLConnector) -> None:
    tables = pg_connector.get_tables("atlas_test")
    table_names = {table.name for table in tables}
    assert {"customers", "orders", "order_items", "products"} <= table_names


def test_get_tables_returns_views_and_materialized_views(
    pg_connector: PostgreSQLConnector,
) -> None:
    tables = {table.name: table for table in pg_connector.get_tables("atlas_test")}
    assert tables["v_active_customers"].table_type is TableType.VIEW
    assert tables["mv_order_summary"].table_type is TableType.MATERIALIZED_VIEW


def test_get_table_comment_is_loaded(pg_connector: PostgreSQLConnector) -> None:
    tables = {table.name: table for table in pg_connector.get_tables("atlas_test")}
    assert tables["customers"].comment == "Customer registry"


def test_get_row_count_estimate_and_size_bytes_are_positive(
    pg_connector: PostgreSQLConnector,
) -> None:
    assert pg_connector.get_row_count_estimate("atlas_test", "orders") >= 4
    assert pg_connector.get_table_size_bytes("atlas_test", "orders") > 0


def test_get_columns_returns_pk_defaults_and_comments(pg_connector: PostgreSQLConnector) -> None:
    columns = {column.name: column for column in pg_connector.get_columns("atlas_test", "customers")}
    assert columns["id"].is_primary_key is True
    assert columns["id"].is_auto_increment is True
    assert columns["email"].comment == "Customer email address"
    assert columns["email"].native_type.startswith("character varying")


def test_get_columns_preserves_nullability_and_jsonb(pg_connector: PostgreSQLConnector) -> None:
    columns = {column.name: column for column in pg_connector.get_columns("atlas_test", "products")}
    assert columns["metadata"].canonical_type.value == "json"
    assert columns["metadata"].is_nullable is True
