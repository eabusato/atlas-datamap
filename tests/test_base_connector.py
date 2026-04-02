"""Unit tests for BaseConnector orchestration and privacy helpers."""

from __future__ import annotations

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors.base import BaseConnector, PrivacyViolationError
from atlas.types import ColumnInfo, ForeignKeyInfo, IndexInfo, SchemaInfo, TableInfo, TableType


class StubConnector(BaseConnector):
    """Fixed-data connector used to test base orchestration logic."""

    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def get_schemas(self) -> list[SchemaInfo]:
        names = ["public", "app"]
        return [
            SchemaInfo(name=name, engine="stub")
            for name in names
            if self._should_include_schema(name)
        ]

    def get_tables(self, schema: str) -> list[TableInfo]:
        if schema != "public":
            return []
        return [
            TableInfo(name="customers", schema=schema, table_type=TableType.TABLE),
            TableInfo(name="orders", schema=schema, table_type=TableType.TABLE),
            TableInfo(name="v_summary", schema=schema, table_type=TableType.VIEW),
        ]

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        if table == "customers":
            return [
                ColumnInfo(
                    name="id",
                    native_type="integer",
                    is_primary_key=True,
                    is_nullable=False,
                    ordinal=1,
                ),
                ColumnInfo(name="email", native_type="varchar", is_nullable=False, ordinal=2),
                ColumnInfo(name="display_name", native_type="varchar", ordinal=3),
            ]
        if table == "orders":
            return [
                ColumnInfo(
                    name="id",
                    native_type="integer",
                    is_primary_key=True,
                    is_nullable=False,
                    ordinal=1,
                ),
                ColumnInfo(name="customer_id", native_type="integer", is_nullable=False, ordinal=2),
                ColumnInfo(name="total", native_type="numeric", ordinal=3),
            ]
        return []

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        if table == "orders":
            return [
                ForeignKeyInfo(
                    name="fk_orders_customer",
                    source_schema=schema,
                    source_table="orders",
                    source_columns=["customer_id"],
                    target_schema=schema,
                    target_table="customers",
                    target_columns=["id"],
                )
            ]
        return []

    def get_indexes(self, schema: str, table: str) -> list[IndexInfo]:
        if table == "customers":
            return [
                IndexInfo(
                    name="customers_pkey",
                    table=table,
                    schema=schema,
                    columns=["id"],
                    is_unique=True,
                    is_primary=True,
                ),
                IndexInfo(
                    name="customers_email_idx",
                    table=table,
                    schema=schema,
                    columns=["email"],
                    is_unique=True,
                ),
            ]
        return []

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        return {"customers": 1000, "orders": 50_000}.get(table, 0)

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        return {"customers": 524_288, "orders": 10_485_760}.get(table, 0)

    def get_sample_rows(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        privacy_mode: PrivacyMode | None = None,
    ) -> list[dict[str, str | None]]:
        effective_mode = self._check_sample_allowed(privacy_mode)
        rows = [
            {"id": "1", "email": "user@example.com", "display_name": "Alex"},
            {"id": "2", "email": "ana@example.com", "display_name": "Ana"},
        ]
        return [self._mask_row(row, effective_mode) for row in rows]

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        return 100


def _make_config(**overrides: object) -> AtlasConnectionConfig:
    payload: dict[str, object] = {
        "engine": DatabaseEngine.postgresql,
        "host": "stub",
        "database": "stubdb",
    }
    payload.update(overrides)
    return AtlasConnectionConfig(**payload)


class TestLifecycle:
    def test_session_context_manager_toggles_connection_state(self) -> None:
        connector = StubConnector(_make_config())
        assert not connector.is_connected
        with connector.session():
            assert connector.is_connected
        assert not connector.is_connected

    def test_session_disconnects_on_exception(self) -> None:
        connector = StubConnector(_make_config())
        with pytest.raises(RuntimeError, match="boom"), connector.session():
            raise RuntimeError("boom")
        assert not connector.is_connected

    def test_repr_contains_connection_status(self) -> None:
        connector = StubConnector(_make_config())
        assert "disconnected" in repr(connector)


class TestSchemaFilter:
    def test_no_filter_returns_all_schemas(self) -> None:
        connector = StubConnector(_make_config())
        with connector.session():
            schemas = connector.get_schemas()
        assert {schema.name for schema in schemas} == {"public", "app"}

    def test_schema_filter_limits_results(self) -> None:
        connector = StubConnector(_make_config(schema_filter=["public"]))
        with connector.session():
            schemas = connector.get_schemas()
        assert [schema.name for schema in schemas] == ["public"]

    def test_schema_exclude_removes_requested_schema(self) -> None:
        connector = StubConnector(_make_config(schema_exclude=["app"]))
        with connector.session():
            schemas = connector.get_schemas()
        assert all(schema.name != "app" for schema in schemas)


class TestIntrospection:
    def test_introspect_schema_populates_columns_and_stats(self) -> None:
        connector = StubConnector(_make_config())
        with connector.session():
            schema = connector.introspect_schema("public")
        orders = next(table for table in schema.tables if table.name == "orders")
        assert len(orders.columns) == 3
        assert orders.row_count_estimate == 50_000
        assert orders.size_bytes == 10_485_760

    def test_introspect_schema_marks_indexed_columns(self) -> None:
        connector = StubConnector(_make_config())
        with connector.session():
            schema = connector.introspect_schema("public")
        customers = next(table for table in schema.tables if table.name == "customers")
        email = next(column for column in customers.columns if column.name == "email")
        assert email.is_indexed

    def test_introspect_schema_marks_fk_columns(self) -> None:
        connector = StubConnector(_make_config())
        with connector.session():
            schema = connector.introspect_schema("public")
        orders = next(table for table in schema.tables if table.name == "orders")
        customer_id = next(column for column in orders.columns if column.name == "customer_id")
        assert customer_id.is_foreign_key

    def test_introspect_all_computes_fk_in_degree_map(self) -> None:
        connector = StubConnector(_make_config(schema_filter=["public"]))
        with connector.session():
            result = connector.introspect_all()
        assert result.fk_in_degree_map["public.customers"] == ["public.orders"]
        customers = result.get_table("public", "customers")
        assert customers is not None
        assert customers.fk_in_degree == 1

    def test_get_column_stats_uses_null_and_distinct_estimates(self) -> None:
        connector = StubConnector(_make_config())
        with connector.session():
            stats = connector.get_column_stats("public", "customers", "email")
        assert stats.row_count == 1000
        assert stats.null_count == 0
        assert stats.distinct_count == 100


class TestPrivacy:
    def test_no_samples_mode_raises(self) -> None:
        connector = StubConnector(_make_config(privacy_mode=PrivacyMode.no_samples))
        with pytest.raises(PrivacyViolationError), connector.session():
            connector.get_sample_rows("public", "customers")

    def test_stats_only_mode_raises(self) -> None:
        connector = StubConnector(_make_config(privacy_mode=PrivacyMode.stats_only))
        with pytest.raises(PrivacyViolationError), connector.session():
            connector.get_sample_rows("public", "customers")

    def test_masked_mode_hides_sensitive_values(self) -> None:
        connector = StubConnector(_make_config(privacy_mode=PrivacyMode.masked))
        with connector.session():
            rows = connector.get_sample_rows("public", "customers")
        for row in rows:
            assert row["email"] == "***"
            assert row["display_name"] != "***"

    def test_normal_mode_keeps_real_values(self) -> None:
        connector = StubConnector(_make_config(privacy_mode=PrivacyMode.normal))
        with connector.session():
            rows = connector.get_sample_rows("public", "customers")
        assert any("@" in (row.get("email") or "") for row in rows)
