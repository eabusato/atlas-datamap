"""Unit tests for canonical Atlas metadata types."""

from __future__ import annotations

import json

import pytest

from atlas.types import (
    AtlasType,
    ColumnInfo,
    ColumnStats,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)


class TestAtlasType:
    @pytest.mark.parametrize(
        ("native_type", "expected"),
        [
            ("integer", AtlasType.INTEGER),
            ("int4", AtlasType.INTEGER),
            ("bigint", AtlasType.BIGINT),
            ("serial", AtlasType.INTEGER),
            ("varchar(255)", AtlasType.TEXT),
            ("character varying", AtlasType.TEXT),
            ("text", AtlasType.CLOB),
            ("nvarchar(max)", AtlasType.TEXT),
            ("numeric(10,2)", AtlasType.DECIMAL),
            ("float", AtlasType.FLOAT),
            ("money", AtlasType.MONEY),
            ("boolean", AtlasType.BOOLEAN),
            ("bool", AtlasType.BOOLEAN),
            ("bit", AtlasType.BOOLEAN),
            ("uuid", AtlasType.UUID),
            ("uniqueidentifier", AtlasType.UUID),
            ("json", AtlasType.JSON),
            ("jsonb", AtlasType.JSON),
            ("timestamp", AtlasType.DATETIME),
            ("datetime2", AtlasType.DATETIME),
            ("date", AtlasType.DATE),
            ("time", AtlasType.TIME),
            ("bytea", AtlasType.BINARY),
            ("varbinary", AtlasType.BINARY),
            ("image", AtlasType.BINARY),
            ("enum", AtlasType.ENUM),
            ("user-defined", AtlasType.UNKNOWN),
            ("nonexistent_xyz", AtlasType.UNKNOWN),
        ],
    )
    def test_from_native_maps_expected_type(self, native_type: str, expected: AtlasType) -> None:
        assert AtlasType.from_native(native_type) is expected


class TestColumnStats:
    def test_null_rate_is_zero_when_row_count_is_zero(self) -> None:
        stats = ColumnStats()
        assert stats.null_rate == 0.0
        assert stats.fill_rate == 1.0

    def test_null_rate_and_fill_rate_are_computed(self) -> None:
        stats = ColumnStats(row_count=100, null_count=25)
        assert stats.null_rate == pytest.approx(0.25)
        assert stats.fill_rate == pytest.approx(0.75)

    def test_null_rate_is_capped_at_one(self) -> None:
        stats = ColumnStats(row_count=10, null_count=15)
        assert stats.null_rate == 1.0

    def test_roundtrip_serialization(self) -> None:
        original = ColumnStats(
            row_count=1000,
            null_count=50,
            distinct_count=200,
            min_value="a",
            max_value="z",
            avg_length=12.5,
        )
        assert ColumnStats.from_dict(original.to_dict()) == original


class TestColumnInfo:
    def test_canonical_type_is_auto_inferred_when_missing(self) -> None:
        column = ColumnInfo(name="id", native_type="integer")
        assert column.canonical_type is AtlasType.INTEGER

    def test_explicit_unknown_canonical_type_is_preserved(self) -> None:
        column = ColumnInfo(
            name="custom",
            native_type="integer",
            canonical_type=AtlasType.UNKNOWN,
        )
        assert column.canonical_type is AtlasType.UNKNOWN

    def test_sensitive_name_detection_uses_patterns(self) -> None:
        assert ColumnInfo(name="user_email", native_type="text").is_sensitive_name
        assert ColumnInfo(name="PASSWORD", native_type="text").is_sensitive_name
        assert ColumnInfo(name="secret_key", native_type="text").is_sensitive_name
        assert not ColumnInfo(name="display_name", native_type="text").is_sensitive_name

    def test_roundtrip_serialization(self) -> None:
        column = ColumnInfo(
            name="email",
            native_type="character varying(255)",
            ordinal=3,
            is_nullable=False,
            is_unique=True,
            comment="User email",
            stats=ColumnStats(row_count=500, null_count=0, distinct_count=500),
        )
        restored = ColumnInfo.from_dict(column.to_dict())
        assert restored.name == column.name
        assert restored.native_type == column.native_type
        assert restored.canonical_type is AtlasType.TEXT
        assert restored.stats.row_count == 500


class TestForeignKeyInfo:
    def test_reference_helpers(self) -> None:
        foreign_key = ForeignKeyInfo(
            name="fk_orders_customer",
            source_schema="public",
            source_table="orders",
            source_columns=["customer_id"],
            target_schema="public",
            target_table="customers",
            target_columns=["id"],
        )
        assert foreign_key.source_ref == "public.orders(customer_id)"
        assert foreign_key.target_ref == "public.customers(id)"

    def test_roundtrip_serialization(self) -> None:
        foreign_key = ForeignKeyInfo(
            name="fk_test",
            source_schema="app",
            source_table="items",
            source_columns=["order_id"],
            target_schema="app",
            target_table="orders",
            target_columns=["id"],
            on_delete="CASCADE",
        )
        assert ForeignKeyInfo.from_dict(foreign_key.to_dict()) == foreign_key


class TestTableInfo:
    def _make_table(self) -> TableInfo:
        return TableInfo(
            name="orders",
            schema="public",
            table_type=TableType.TABLE,
            row_count_estimate=50_000,
            size_bytes=10_485_760,
            columns=[
                ColumnInfo(
                    name="id", native_type="integer", is_primary_key=True, is_nullable=False
                ),
                ColumnInfo(
                    name="customer_id",
                    native_type="integer",
                    is_foreign_key=True,
                    is_nullable=False,
                ),
                ColumnInfo(name="total", native_type="numeric"),
                ColumnInfo(name="created_at", native_type="timestamp"),
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
        )

    def test_qualified_name(self) -> None:
        table = self._make_table()
        assert table.qualified_name == "public.orders"

    def test_primary_key_columns(self) -> None:
        table = self._make_table()
        primary_keys = table.primary_key_columns
        assert len(primary_keys) == 1
        assert primary_keys[0].name == "id"

    def test_size_bytes_human(self) -> None:
        assert (
            TableInfo(name="table", schema="schema", size_bytes=10_485_760).size_bytes_human
            == "10.0 MB"
        )
        assert (
            TableInfo(name="table", schema="schema", size_bytes=2048).size_bytes_human == "2.0 KB"
        )
        assert TableInfo(name="table", schema="schema", size_bytes=0).size_bytes_human == "unknown"

    def test_roundtrip_serialization(self) -> None:
        original = self._make_table()
        restored = TableInfo.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.schema == original.schema
        assert restored.table_type is TableType.TABLE
        assert len(restored.columns) == 4
        assert len(restored.foreign_keys) == 1


class TestSchemaAndResult:
    def _make_result(self) -> IntrospectionResult:
        column = ColumnInfo(name="id", native_type="integer")
        table = TableInfo(name="orders", schema="public", columns=[column])
        schema = SchemaInfo(name="public", engine="postgresql", tables=[table])
        return IntrospectionResult(
            database="mydb",
            engine="postgresql",
            host="localhost",
            schemas=[schema],
        )

    def test_result_summary_is_computed(self) -> None:
        result = self._make_result()
        assert result.total_tables == 1
        assert result.total_columns == 1

    def test_get_table_returns_expected_table(self) -> None:
        result = self._make_result()
        table = result.get_table("public", "orders")
        assert table is not None
        assert table.name == "orders"

    def test_get_table_returns_none_when_missing(self) -> None:
        result = self._make_result()
        assert result.get_table("public", "payments") is None

    def test_all_tables_returns_flat_list(self) -> None:
        result = self._make_result()
        assert len(result.all_tables()) == 1

    def test_json_roundtrip_preserves_fields(self) -> None:
        original = self._make_result()
        payload = original.to_json()
        assert json.loads(payload)["database"] == "mydb"
        restored = IntrospectionResult.from_json(payload)
        assert restored.database == original.database
        assert restored.total_tables == original.total_tables
