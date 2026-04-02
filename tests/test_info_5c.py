"""Phase 5C unit tests for selective table inspection and formatting."""

from __future__ import annotations

import json
from typing import Any

import pytest
from click.testing import CliRunner

from atlas.cli._info_format import render_json, render_text, render_yaml
from atlas.cli.info import TableNotFoundError, _fetch_table_info, _parse_table_ref, info_cmd
from atlas.config import AtlasConnectionConfig, DatabaseEngine
from atlas.connectors.base import BaseConnector
from atlas.types import ColumnInfo, ForeignKeyInfo, IndexInfo, TableInfo, TableType


def _config() -> AtlasConnectionConfig:
    return AtlasConnectionConfig(engine=DatabaseEngine.postgresql, host="localhost", database="atlas")


class _InfoConnector(BaseConnector):
    def __init__(self, config: AtlasConnectionConfig) -> None:
        super().__init__(config)
        self.calls: list[str] = []

    def connect(self) -> None:
        self.calls.append("connect")
        self._connected = True

    def disconnect(self) -> None:
        self.calls.append("disconnect")
        self._connected = False

    def get_schemas(self) -> list[Any]:
        return []

    def get_tables(self, schema: str) -> list[TableInfo]:
        self.calls.append(f"tables:{schema}")
        return [
            TableInfo(name="orders", schema=schema, table_type=TableType.TABLE, comment="Order facts"),
            TableInfo(name="customers", schema=schema, table_type=TableType.TABLE),
        ]

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        self.calls.append(f"rows:{schema}.{table}")
        return 5

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        self.calls.append(f"size:{schema}.{table}")
        return 4096

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        self.calls.append(f"columns:{schema}.{table}")
        return [
            ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="customer_id", native_type="integer", is_nullable=False),
        ]

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        self.calls.append(f"fks:{schema}.{table}")
        return [
            ForeignKeyInfo(
                name="fk_orders_customer",
                source_schema=schema,
                source_table=table,
                source_columns=["customer_id"],
                target_schema=schema,
                target_table="customers",
                target_columns=["id"],
            )
        ]

    def get_indexes(self, schema: str, table: str) -> list[IndexInfo]:
        self.calls.append(f"indexes:{schema}.{table}")
        return [
            IndexInfo(
                name="orders_pkey",
                table=table,
                schema=schema,
                columns=["id"],
                is_primary=True,
                is_unique=True,
            )
        ]

    def get_sample_rows(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        privacy_mode: object | None = None,
    ) -> list[dict[str, Any]]:
        return []

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        return 0


def _table_info() -> TableInfo:
    table = TableInfo(
        name="orders",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=50231,
        size_bytes=4 * (1 << 20),
        comment="Order facts",
        columns=[
            ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="customer_id", native_type="integer", is_nullable=False, is_foreign_key=True),
        ],
        foreign_keys=[
            ForeignKeyInfo(
                name="fk_orders_customer",
                source_schema="public",
                source_table="orders",
                source_columns=["customer_id"],
                target_schema="public",
                target_table="customers",
                target_columns=["id"],
            )
        ],
        indexes=[
            IndexInfo(name="orders_pkey", table="orders", schema="public", columns=["id"], is_primary=True, is_unique=True),
        ],
    )
    table.column_count = len(table.columns)
    return table


def test_render_text_includes_header_and_sections() -> None:
    rendered = render_text(_table_info())

    assert "public.orders" in rendered
    assert "COLUMNS (2)" in rendered
    assert "FOREIGN KEYS (1)" in rendered
    assert "INDEXES (1)" in rendered


def test_render_json_omits_disabled_sections() -> None:
    payload = json.loads(render_json(_table_info(), include_columns=False, include_indexes=False))

    assert "columns" not in payload
    assert "indexes" not in payload
    assert "foreign_keys" in payload


def test_render_yaml_emits_nested_keys() -> None:
    rendered = render_yaml(_table_info())

    assert "name: orders" in rendered
    assert "foreign_keys:" in rendered
    assert "indexes:" in rendered


def test_parse_table_ref_supports_schema_and_default_schema() -> None:
    assert _parse_table_ref("public.orders") == ("public", "orders")
    assert _parse_table_ref('"audit"."events"') == ("audit", "events")
    assert _parse_table_ref("orders") == ("public", "orders")


def test_fetch_table_info_uses_selective_calls() -> None:
    connector = _InfoConnector(_config())

    info = _fetch_table_info(
        connector,
        "public",
        "orders",
        include_columns=False,
        include_fks=True,
        include_indexes=False,
    )

    assert info.name == "orders"
    assert info.columns == []
    assert len(info.foreign_keys) == 1
    assert info.indexes == []
    assert "columns:public.orders" not in connector.calls
    assert "indexes:public.orders" not in connector.calls


def test_fetch_table_info_raises_for_missing_table() -> None:
    connector = _InfoConnector(_config())

    with pytest.raises(TableNotFoundError, match="public.missing"):
        _fetch_table_info(connector, "public", "missing")


def test_info_cmd_renders_text_output(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.cli.info.get_connector", lambda config: _InfoConnector(config))

    result = CliRunner().invoke(
        info_cmd,
        ["--db", "postgresql://localhost/atlas", "--table", "public.orders"],
    )

    assert result.exit_code == 0, result.output
    assert "public.orders" in result.output
    assert "COLUMNS (2)" in result.output


def test_info_cmd_renders_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.cli.info.get_connector", lambda config: _InfoConnector(config))

    result = CliRunner().invoke(
        info_cmd,
        ["--db", "postgresql://localhost/atlas", "--table", "public.orders", "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["name"] == "orders"
    assert payload["schema"] == "public"


def test_info_cmd_returns_error_for_missing_table(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.cli.info.get_connector", lambda config: _InfoConnector(config))

    result = CliRunner().invoke(
        info_cmd,
        ["--db", "postgresql://localhost/atlas", "--table", "public.missing"],
    )

    assert result.exit_code != 0
    assert "public.missing" in result.output
