"""Phase 7A unit tests for textual metadata search."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from atlas.cli.search import search_cmd
from atlas.config import AtlasConnectionConfig, DatabaseEngine
from atlas.connectors.base import BaseConnector
from atlas.search import AtlasSearch
from atlas.search.types import EntityType, SearchResult
from atlas.types import (
    AtlasType,
    ColumnInfo,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)


def _result() -> IntrospectionResult:
    return IntrospectionResult(
        database="atlas",
        engine="postgresql",
        host="localhost",
        schemas=[
            SchemaInfo(
                name="sales_ops",
                engine="postgresql",
                tables=[
                    TableInfo(
                        name="CustomerOrders",
                        schema="sales_ops",
                        table_type=TableType.TABLE,
                        comment="Customer order facts and payments",
                        heuristic_type="fact",
                        columns=[
                            ColumnInfo(
                                name="id",
                                native_type="bigint",
                                is_primary_key=True,
                                is_nullable=False,
                            ),
                            ColumnInfo(
                                name="customer_id",
                                native_type="bigint",
                                is_nullable=False,
                                is_foreign_key=True,
                            ),
                            ColumnInfo(
                                name="payment_status",
                                native_type="varchar(32)",
                                comment="Payment lifecycle marker",
                            ),
                        ],
                        foreign_keys=[
                            ForeignKeyInfo(
                                name="fk_customer_orders_customer",
                                source_schema="sales_ops",
                                source_table="CustomerOrders",
                                source_columns=["customer_id"],
                                target_schema="sales_ops",
                                target_table="CustomerAccount",
                                target_columns=["id"],
                            )
                        ],
                    ),
                    TableInfo(
                        name="CustomerAccount",
                        schema="sales_ops",
                        table_type=TableType.TABLE,
                        comment="Customer master data",
                        heuristic_type="dimension",
                        columns=[
                            ColumnInfo(
                                name="id",
                                native_type="bigint",
                                is_primary_key=True,
                                is_nullable=False,
                            ),
                            ColumnInfo(
                                name="email_address",
                                native_type="varchar(255)",
                                canonical_type=AtlasType.TEXT,
                                comment="Primary customer email",
                            ),
                        ],
                    ),
                ],
            )
        ],
    )


class _SearchConnector(BaseConnector):
    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def get_schemas(self) -> list[SchemaInfo]:
        return _result().schemas

    def get_tables(self, schema: str) -> list[TableInfo]:
        schema_info = _result().get_schema(schema)
        assert schema_info is not None
        return schema_info.tables

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        table_info = _result().get_table(schema, table)
        assert table_info is not None
        return table_info.columns

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        table_info = _result().get_table(schema, table)
        assert table_info is not None
        return table_info.foreign_keys

    def get_indexes(self, schema: str, table: str) -> list[object]:
        return []

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        return 0

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        return 0

    def get_sample_rows(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        privacy_mode: object | None = None,
    ) -> list[dict[str, object]]:
        return []

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        return 0


def _config() -> AtlasConnectionConfig:
    return AtlasConnectionConfig(
        engine=DatabaseEngine.postgresql,
        host="localhost",
        database="atlas",
    )


def test_search_result_qualified_name_and_to_dict() -> None:
    result = SearchResult(
        entity_type=EntityType.COLUMN,
        schema="public",
        table="orders",
        column="customer_id",
        score=17.333,
        reason="match",
    )

    assert result.qualified_name == "public.orders.customer_id"
    assert result.to_dict()["score"] == 17.333


def test_normalize_tokens_splits_camel_and_snake_case() -> None:
    search = AtlasSearch(_result())

    assert search._normalize_tokens("CustomerOrders customer_orders") == {"customer", "orders"}


def test_search_tables_prefers_exact_name_token_set() -> None:
    search = AtlasSearch(_result())

    results = search.search_tables("customer orders")

    assert results[0].qualified_name == "sales_ops.CustomerOrders"
    assert "L0 exact name token-set" in results[0].reason


def test_search_tables_can_filter_by_heuristic_type() -> None:
    search = AtlasSearch(_result())

    results = search.search_tables("customer", type_filter="dimension")

    assert [item.qualified_name for item in results] == ["sales_ops.CustomerAccount"]


def test_search_columns_uses_comment_and_type_metadata() -> None:
    search = AtlasSearch(_result())

    results = search.search_columns("email")

    assert results[0].entity_type is EntityType.COLUMN
    assert results[0].qualified_name == "sales_ops.CustomerAccount.email_address"


def test_search_schema_combines_schema_table_and_column_hits() -> None:
    search = AtlasSearch(_result())

    results = search.search_schema("sales customer")

    assert any(item.entity_type is EntityType.SCHEMA for item in results)
    assert any(item.entity_type is EntityType.TABLE for item in results)


def test_search_cli_renders_ranked_results(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.cli.search.get_connector", lambda config: _SearchConnector(config))
    monkeypatch.setattr("atlas.cli.search.TableClassifier.classify_all", lambda self, result: result)

    result = CliRunner().invoke(
        search_cmd,
        ["--db", "postgresql://localhost/atlas", "customer orders"],
    )

    assert result.exit_code == 0, result.output
    assert "[table] sales_ops.CustomerOrders" in result.output


def test_search_cli_rejects_type_with_columns() -> None:
    result = CliRunner().invoke(
        search_cmd,
        ["--type", "fact", "--columns", "customer_id"],
    )

    assert result.exit_code != 0
    assert "--type cannot be combined with --columns" in result.output
