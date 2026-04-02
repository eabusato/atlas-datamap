"""Unit tests for Phase 1B PostgreSQL table and column introspection."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine
from atlas.connectors.postgresql import PostgreSQLConnector, _compose_native_type
from atlas.types import AtlasType, TableType


def _make_config(**overrides: Any) -> AtlasConnectionConfig:
    payload = {
        "engine": DatabaseEngine.postgresql,
        "host": "localhost",
        "database": "atlas_test",
    }
    payload.update(overrides)
    return AtlasConnectionConfig(**payload)


def _make_connector_with_fetch_sequence(results: list[Any]) -> PostgreSQLConnector:
    connector = PostgreSQLConnector(_make_config())
    result_iter = iter(results)

    def fake_fetchall(sql: str, params: tuple[Any, ...]) -> list[Any]:
        result = next(result_iter)
        assert isinstance(result, list)
        return result

    def fake_fetchone(sql: str, params: tuple[Any, ...]) -> Any:
        result = next(result_iter)
        assert not isinstance(result, list)
        return result

    connector._fetchall = MagicMock(side_effect=fake_fetchall)  # type: ignore[method-assign]
    connector._fetchone = MagicMock(side_effect=fake_fetchone)  # type: ignore[method-assign]
    connector._connected = True
    return connector


class TestComposeNativeType:
    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (("character varying", "varchar", 255, None, None), "character varying(255)"),
            (("character varying", "varchar", None, None, None), "character varying"),
            (("numeric", "numeric", None, 10, 2), "numeric(10,2)"),
            (("numeric", "numeric", None, None, None), "numeric"),
            (("ARRAY", "_int4", None, None, None), "integer[]"),
            (("ARRAY", "_varchar", None, None, None), "character varying[]"),
            (("USER-DEFINED", "citext", None, None, None), "citext"),
            (("integer", "int4", None, None, None), "integer"),
        ],
    )
    def test_compose_native_type(self, args: tuple[Any, ...], expected: str) -> None:
        assert _compose_native_type(*args) == expected


class TestGetTables:
    def test_returns_base_tables_and_views(self) -> None:
        connector = _make_connector_with_fetch_sequence(
            [
                [("customers", "BASE TABLE", "Customer table"), ("v_orders", "VIEW", None)],
                [("mv_summary", "Summary"), ("mv_finance", None)],
            ]
        )
        tables = connector.get_tables("public")
        by_name = {table.name: table for table in tables}
        assert by_name["customers"].table_type is TableType.TABLE
        assert by_name["v_orders"].table_type is TableType.VIEW
        assert by_name["mv_summary"].table_type is TableType.MATERIALIZED_VIEW
        assert by_name["customers"].comment == "Customer table"

    def test_returns_empty_list_for_empty_schema(self) -> None:
        connector = _make_connector_with_fetch_sequence([[], []])
        assert connector.get_tables("empty") == []

    def test_sorted_by_name(self) -> None:
        connector = _make_connector_with_fetch_sequence(
            [[("zeta", "BASE TABLE", None), ("alpha", "BASE TABLE", None)], []]
        )
        tables = connector.get_tables("public")
        assert [table.name for table in tables] == ["alpha", "zeta"]


class TestCountsAndSizes:
    def test_get_row_count_estimate_returns_integer(self) -> None:
        connector = _make_connector_with_fetch_sequence([(42_000,)])
        assert connector.get_row_count_estimate("public", "orders") == 42_000

    def test_get_row_count_estimate_returns_zero_when_missing(self) -> None:
        connector = _make_connector_with_fetch_sequence([None])
        assert connector.get_row_count_estimate("public", "orders") == 0

    def test_get_table_size_bytes_returns_integer(self) -> None:
        connector = _make_connector_with_fetch_sequence([(2048,)])
        assert connector.get_table_size_bytes("public", "orders") == 2048

    def test_get_table_size_bytes_returns_zero_when_missing(self) -> None:
        connector = _make_connector_with_fetch_sequence([None])
        assert connector.get_table_size_bytes("public", "orders") == 0


class TestGetColumns:
    def test_returns_columns_with_metadata(self) -> None:
        connector = _make_connector_with_fetch_sequence(
            [
                [
                    (
                        "id",
                        "integer",
                        "int4",
                        None,
                        None,
                        None,
                        "NO",
                        "nextval('customers_id_seq'::regclass)",
                        1,
                        True,
                        "Primary key",
                    ),
                    (
                        "email",
                        "character varying",
                        "varchar",
                        255,
                        None,
                        None,
                        "NO",
                        None,
                        2,
                        False,
                        "Unique email",
                    ),
                ]
            ]
        )
        columns = connector.get_columns("public", "customers")
        by_name = {column.name: column for column in columns}
        assert by_name["id"].is_primary_key is True
        assert by_name["id"].is_auto_increment is True
        assert by_name["id"].canonical_type is AtlasType.INTEGER
        assert by_name["email"].native_type == "character varying(255)"
        assert by_name["email"].canonical_type is AtlasType.TEXT
        assert by_name["email"].comment == "Unique email"

    def test_nullable_and_default_fields(self) -> None:
        connector = _make_connector_with_fetch_sequence(
            [
                [
                    (
                        "status",
                        "character varying",
                        "varchar",
                        20,
                        None,
                        None,
                        "YES",
                        "'pending'::character varying",
                        3,
                        False,
                        None,
                    )
                ]
            ]
        )
        columns = connector.get_columns("public", "orders")
        assert columns[0].is_nullable is True
        assert columns[0].default_value == "'pending'::character varying"

    def test_column_order_preserved(self) -> None:
        connector = _make_connector_with_fetch_sequence(
            [
                [
                    ("a", "integer", "int4", None, None, None, "NO", None, 1, False, None),
                    ("b", "integer", "int4", None, None, None, "YES", None, 2, False, None),
                ]
            ]
        )
        columns = connector.get_columns("public", "t")
        assert [column.name for column in columns] == ["a", "b"]

    def test_user_defined_and_array_types_are_composed(self) -> None:
        connector = _make_connector_with_fetch_sequence(
            [
                [
                    ("tags", "ARRAY", "_varchar", None, None, None, "YES", None, 1, False, None),
                    ("search_name", "USER-DEFINED", "citext", None, None, None, "YES", None, 2, False, None),
                ]
            ]
        )
        columns = connector.get_columns("public", "products")
        assert columns[0].native_type == "character varying[]"
        assert columns[0].canonical_type is AtlasType.ARRAY
        assert columns[1].native_type == "citext"
        assert columns[1].canonical_type is AtlasType.TEXT
