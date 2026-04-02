"""Helpers shared by Phase 0 integration tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors.base import BaseConnector
from atlas.types import ColumnInfo, ForeignKeyInfo, IndexInfo, SchemaInfo, TableInfo, TableType


class IntegrationStubConnector(BaseConnector):
    """Integration-grade fixed connector for cross-module testing."""

    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def get_schemas(self) -> list[SchemaInfo]:
        return (
            [SchemaInfo(name="public", engine="stub")]
            if self._should_include_schema("public")
            else []
        )

    def get_tables(self, schema: str) -> list[TableInfo]:
        return [
            TableInfo(name="customers", schema=schema, table_type=TableType.TABLE),
            TableInfo(name="orders", schema=schema, table_type=TableType.TABLE),
        ]

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        if table == "customers":
            return [
                ColumnInfo(
                    name="id", native_type="integer", is_primary_key=True, is_nullable=False
                ),
                ColumnInfo(name="customer_email", native_type="text", is_nullable=False),
            ]
        return [
            ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="customer_id", native_type="integer", is_nullable=False),
            ColumnInfo(name="total", native_type="numeric"),
        ]

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        if table != "orders":
            return []
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

    def get_indexes(self, schema: str, table: str) -> list[IndexInfo]:
        if table != "customers":
            return []
        return [
            IndexInfo(
                name="customers_email_idx",
                table=table,
                schema=schema,
                columns=["customer_email"],
                is_unique=True,
            )
        ]

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        return {"customers": 2, "orders": 3}[table]

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        return {"customers": 128, "orders": 256}[table]

    def get_sample_rows(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        privacy_mode: PrivacyMode | None = None,
    ) -> list[dict[str, str | None]]:
        effective_mode = self._check_sample_allowed(privacy_mode)
        rows = [{"id": "1", "customer_email": "user@example.com"}]
        return [self._mask_row(row, effective_mode) for row in rows]

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        return 2


def make_config(**overrides: object) -> AtlasConnectionConfig:
    payload: dict[str, object] = {
        "engine": DatabaseEngine.postgresql,
        "host": "stub",
        "database": "stubdb",
    }
    payload.update(overrides)
    return AtlasConnectionConfig(**payload)


def build_sqlite_fixture(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL UNIQUE
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                total NUMERIC,
                FOREIGN KEY (customer_id) REFERENCES customers(id)
            );
            INSERT INTO customers (id, email) VALUES (1, 'user@example.com');
            INSERT INTO orders (id, customer_id, total) VALUES (10, 1, 42.50);
            """
        )
        connection.commit()
    finally:
        connection.close()
