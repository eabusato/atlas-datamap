"""Unit tests for Phase 1C PostgreSQL foreign keys and indexes."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from atlas.config import AtlasConnectionConfig, DatabaseEngine
from atlas.connectors.postgresql import PostgreSQLConnector
from atlas.types import AtlasType, ColumnInfo, ForeignKeyInfo, IndexInfo


def _make_config(**overrides: Any) -> AtlasConnectionConfig:
    payload = {
        "engine": DatabaseEngine.postgresql,
        "host": "localhost",
        "database": "atlas_test",
    }
    payload.update(overrides)
    return AtlasConnectionConfig(**payload)


def _make_connector_with_fetchall(rows: list[Any]) -> PostgreSQLConnector:
    connector = PostgreSQLConnector(_make_config())
    connector._fetchall = MagicMock(return_value=rows)  # type: ignore[method-assign]
    connector._connected = True
    return connector


class TestGetForeignKeys:
    def test_single_foreign_key(self) -> None:
        connector = _make_connector_with_fetchall(
            [
                (
                    "fk_orders_customer",
                    "public",
                    "orders",
                    ["customer_id"],
                    "public",
                    "customers",
                    ["id"],
                    "NO ACTION",
                    "CASCADE",
                )
            ]
        )
        foreign_keys = connector.get_foreign_keys("public", "orders")
        assert len(foreign_keys) == 1
        assert foreign_keys[0].target_table == "customers"
        assert foreign_keys[0].source_columns == ["customer_id"]

    def test_composite_foreign_key(self) -> None:
        connector = _make_connector_with_fetchall(
            [
                (
                    "fk_composite",
                    "public",
                    "order_items",
                    ["order_id", "product_id"],
                    "public",
                    "orders",
                    ["id", "product_id"],
                    "CASCADE",
                    "NO ACTION",
                )
            ]
        )
        foreign_keys = connector.get_foreign_keys("public", "order_items")
        assert foreign_keys[0].source_columns == ["order_id", "product_id"]
        assert foreign_keys[0].target_columns == ["id", "product_id"]

    def test_no_foreign_keys_returns_empty_list(self) -> None:
        assert _make_connector_with_fetchall([]).get_foreign_keys("public", "standalone") == []


class TestInferImplicitFks:
    def _column(self, name: str, canonical_type: AtlasType = AtlasType.INTEGER) -> ColumnInfo:
        column = ColumnInfo(name=name, native_type="integer")
        column.canonical_type = canonical_type
        return column

    def test_infers_customer_id_against_plural_table(self) -> None:
        connector = PostgreSQLConnector(_make_config())
        inferred = connector.infer_implicit_fks(
            "public",
            "orders",
            [self._column("customer_id")],
            {"customers", "orders"},
            [],
        )
        assert len(inferred) == 1
        assert inferred[0].target_table == "customers"
        assert inferred[0].is_inferred is True

    def test_does_not_infer_when_declared_fk_exists(self) -> None:
        connector = PostgreSQLConnector(_make_config())
        declared = [
            ForeignKeyInfo(
                name="fk_orders_customer",
                source_schema="public",
                source_table="orders",
                source_columns=["customer_id"],
                target_schema="public",
                target_table="customers",
                target_columns=["id"],
            )
        ]
        inferred = connector.infer_implicit_fks(
            "public",
            "orders",
            [self._column("customer_id")],
            {"customers"},
            declared,
        )
        assert inferred == []

    def test_does_not_infer_for_non_integer_columns(self) -> None:
        connector = PostgreSQLConnector(_make_config())
        inferred = connector.infer_implicit_fks(
            "public",
            "orders",
            [self._column("customer_id", AtlasType.TEXT)],
            {"customers"},
            [],
        )
        assert inferred == []

    def test_portuguese_plural_is_supported(self) -> None:
        connector = PostgreSQLConnector(_make_config())
        inferred = connector.infer_implicit_fks(
            "public",
            "produtos",
            [self._column("categoria_id")],
            {"categorias"},
            [],
        )
        assert inferred[0].target_table == "categorias"


class TestGetIndexes:
    def test_primary_and_regular_indexes(self) -> None:
        connector = _make_connector_with_fetchall(
            [
                ("customers_pkey", True, True, "btree", False, ["id"]),
                ("customers_email_idx", True, False, "btree", False, ["email"]),
            ]
        )
        indexes = connector.get_indexes("public", "customers")
        assert indexes[0].is_primary is True
        assert indexes[1].columns == ["email"]

    def test_partial_and_composite_indexes(self) -> None:
        connector = _make_connector_with_fetchall(
            [("orders_status_created_idx", False, False, "btree", True, ["status", "created_at"])]
        )
        indexes = connector.get_indexes("public", "orders")
        assert indexes[0].is_partial is True
        assert indexes[0].columns == ["status", "created_at"]

    def test_detect_redundant_indexes(self) -> None:
        indexes = [
            IndexInfo(name="idx_customer", table="orders", schema="public", columns=["customer_id"]),
            IndexInfo(
                name="idx_customer_status",
                table="orders",
                schema="public",
                columns=["customer_id", "status"],
            ),
            IndexInfo(name="idx_status", table="orders", schema="public", columns=["status"]),
        ]
        redundant = PostgreSQLConnector.detect_redundant_indexes(indexes)
        assert "idx_customer" in redundant
        assert "idx_status" not in redundant

    def test_partial_indexes_are_ignored_in_redundancy_detection(self) -> None:
        indexes = [
            IndexInfo(name="idx_email", table="customers", schema="public", columns=["email"]),
            IndexInfo(
                name="idx_email_partial",
                table="customers",
                schema="public",
                columns=["email", "active"],
                is_partial=True,
            ),
        ]
        assert PostgreSQLConnector.detect_redundant_indexes(indexes) == []
