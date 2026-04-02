"""Integration tests for Phase 1C PostgreSQL foreign keys and indexes."""

from __future__ import annotations

import pytest

from atlas.connectors.postgresql import PostgreSQLConnector

pytestmark = pytest.mark.integration


def test_get_foreign_keys_returns_declared_relationships(pg_connector: PostgreSQLConnector) -> None:
    foreign_keys = pg_connector.get_foreign_keys("atlas_test", "orders")
    assert any(foreign_key.target_table == "customers" for foreign_key in foreign_keys)


def test_get_foreign_keys_preserves_source_and_target_columns(
    pg_connector: PostgreSQLConnector,
) -> None:
    foreign_keys = pg_connector.get_foreign_keys("atlas_test", "order_items")
    order_fk = next(foreign_key for foreign_key in foreign_keys if foreign_key.target_table == "orders")
    assert order_fk.source_columns == ["order_id"]
    assert order_fk.target_columns == ["id"]


def test_introspect_schema_adds_implicit_fk_for_audit_log(
    pg_connector: PostgreSQLConnector,
) -> None:
    schema = pg_connector.introspect_schema("atlas_test")
    audit_log = next(table for table in schema.tables if table.name == "audit_log")
    assert any(
        foreign_key.is_inferred and foreign_key.target_table == "customers"
        for foreign_key in audit_log.foreign_keys
    )


def test_get_indexes_returns_index_type_and_columns(pg_connector: PostgreSQLConnector) -> None:
    indexes = {index.name: index for index in pg_connector.get_indexes("atlas_test", "orders")}
    assert indexes["idx_orders_customer"].index_type == "btree"
    assert indexes["idx_orders_customer"].columns == ["customer_id"]


def test_detect_redundant_indexes_flags_prefix_index(pg_connector: PostgreSQLConnector) -> None:
    indexes = pg_connector.get_indexes("atlas_test", "orders")
    redundant = pg_connector.detect_redundant_indexes(indexes)
    assert "idx_orders_customer" in redundant


def test_introspect_schema_marks_indexed_and_fk_columns(pg_connector: PostgreSQLConnector) -> None:
    schema = pg_connector.introspect_schema("atlas_test")
    orders = next(table for table in schema.tables if table.name == "orders")
    columns = {column.name: column for column in orders.columns}
    assert columns["customer_id"].is_foreign_key is True
    assert columns["customer_id"].is_indexed is True
