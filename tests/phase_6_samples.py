"""Shared synthetic fixtures for Phase 6 analysis tests."""

from __future__ import annotations

from atlas.types import (
    AtlasType,
    ColumnInfo,
    ColumnStats,
    ForeignKeyInfo,
    IndexInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)


def make_column(
    name: str,
    native_type: str,
    *,
    canonical_type: AtlasType | None = None,
    nullable: bool = True,
    primary_key: bool = False,
    foreign_key: bool = False,
    unique: bool = False,
    indexed: bool = False,
    stats: ColumnStats | None = None,
) -> ColumnInfo:
    return ColumnInfo(
        name=name,
        native_type=native_type,
        canonical_type=canonical_type,
        is_nullable=nullable,
        is_primary_key=primary_key,
        is_foreign_key=foreign_key,
        is_unique=unique,
        is_indexed=indexed,
        stats=stats or ColumnStats(),
    )


def make_fk(
    source_table: str,
    source_columns: list[str],
    target_table: str,
    *,
    schema: str = "public",
    name: str | None = None,
) -> ForeignKeyInfo:
    return ForeignKeyInfo(
        name=name or f"fk_{source_table}_{target_table}_{'_'.join(source_columns)}",
        source_schema=schema,
        source_table=source_table,
        source_columns=source_columns,
        target_schema=schema,
        target_table=target_table,
        target_columns=["id"],
    )


def make_index(
    table: str,
    columns: list[str],
    *,
    schema: str = "public",
    name: str | None = None,
    unique: bool = False,
    primary: bool = False,
) -> IndexInfo:
    return IndexInfo(
        name=name or f"idx_{table}_{'_'.join(columns)}",
        table=table,
        schema=schema,
        columns=columns,
        is_unique=unique,
        is_primary=primary,
    )


def make_table(
    name: str,
    *,
    schema: str = "public",
    table_type: TableType = TableType.TABLE,
    row_count: int = 0,
    comment: str | None = None,
    columns: list[ColumnInfo] | None = None,
    foreign_keys: list[ForeignKeyInfo] | None = None,
    indexes: list[IndexInfo] | None = None,
) -> TableInfo:
    return TableInfo(
        name=name,
        schema=schema,
        table_type=table_type,
        row_count_estimate=row_count,
        comment=comment,
        columns=columns or [],
        foreign_keys=foreign_keys or [],
        indexes=indexes or [],
    )


def make_result(
    tables: list[TableInfo],
    *,
    schema: str = "public",
    engine: str = "sqlite",
    database: str = "atlas",
    host: str = "localhost",
    fk_in_degree_map: dict[str, list[str]] | None = None,
) -> IntrospectionResult:
    return IntrospectionResult(
        database=database,
        engine=engine,
        host=host,
        schemas=[SchemaInfo(name=schema, engine=engine, tables=tables)],
        fk_in_degree_map=fk_in_degree_map or {},
    )
