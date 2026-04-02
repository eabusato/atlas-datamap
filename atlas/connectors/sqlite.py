"""Stdlib-backed SQLite connector for local Atlas introspection."""

from __future__ import annotations

import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from atlas.config import AtlasConnectionConfig, PrivacyMode
from atlas.connectors.base import BaseConnector, ConnectionError, QueryError
from atlas.types import (
    AtlasType,
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)


class SQLiteConnector(BaseConnector):
    """Concrete SQLite connector using ``sqlite3`` and ``PRAGMA`` metadata."""

    def __init__(self, config: AtlasConnectionConfig) -> None:
        super().__init__(config)
        self._connection: sqlite3.Connection | None = None
        self._database_path = Path(self._config.database)

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _qualified_name(self, schema: str, table: str) -> str:
        if schema and schema != "main":
            return f"{self._quote_identifier(schema)}.{self._quote_identifier(table)}"
        return self._quote_identifier(table)

    def connect(self) -> None:
        if self._connected:
            return
        try:
            if self._config.database == ":memory:":
                self._connection = sqlite3.connect(
                    self._config.database,
                    timeout=self._config.timeout_seconds,
                )
            else:
                database_uri = self._database_path.resolve().as_uri()
                self._connection = sqlite3.connect(
                    f"{database_uri}?mode=ro",
                    timeout=self._config.timeout_seconds,
                    uri=True,
                )
            self._connection.row_factory = sqlite3.Row
        except sqlite3.Error as exc:
            raise ConnectionError(
                f"Failed to connect to SQLite database {self._config.database!r}."
            ) from exc
        self._connected = True

    def disconnect(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        self._connected = False

    def _cursor(self) -> sqlite3.Cursor:
        if self._connection is None:
            raise ConnectionError("SQLite connector is not connected.")
        return self._connection.cursor()

    def get_schemas(self) -> list[SchemaInfo]:
        return (
            [SchemaInfo(name="main", engine="sqlite")]
            if self._should_include_schema("main")
            else []
        )

    def get_tables(self, schema: str) -> list[TableInfo]:
        cursor = self._cursor()
        try:
            rows = cursor.execute(
                """
                SELECT name, type
                FROM sqlite_master
                WHERE type IN ('table', 'view')
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            ).fetchall()
        except sqlite3.Error as exc:
            raise QueryError("Failed to fetch SQLite tables.") from exc
        tables: list[TableInfo] = []
        for row in rows:
            table_type = TableType.VIEW if row["type"] == "view" else TableType.TABLE
            tables.append(TableInfo(name=str(row["name"]), schema=schema, table_type=table_type))
        return tables

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        cursor = self._cursor()
        try:
            row = cursor.execute(
                f"SELECT COUNT(*) AS count FROM {self._qualified_name(schema, table)}"
            ).fetchone()
        except sqlite3.Error:
            return 0
        return int(row["count"]) if row is not None else 0

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        return 0

    def _map_sqlite_type(self, native_type: str) -> AtlasType:
        normalized = native_type.strip().upper()
        if not normalized:
            return AtlasType.UNKNOWN
        if "JSON" in normalized:
            return AtlasType.JSON
        if "INT" in normalized:
            return AtlasType.INTEGER
        if "CHAR" in normalized:
            return AtlasType.TEXT
        if "CLOB" in normalized:
            return AtlasType.CLOB
        if "TEXT" in normalized:
            return AtlasType.CLOB
        if "BLOB" in normalized:
            return AtlasType.BINARY
        if "BOOL" in normalized:
            return AtlasType.BOOLEAN
        if "DATETIME" in normalized or ("TIME" in normalized and "STAMP" in normalized):
            return AtlasType.DATETIME
        if "TIME" in normalized:
            return AtlasType.DATETIME
        if "DATE" in normalized:
            return AtlasType.DATE
        if "REAL" in normalized or "FLOA" in normalized or "DOUB" in normalized:
            return AtlasType.FLOAT
        return AtlasType.UNKNOWN

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        cursor = self._cursor()
        try:
            rows = cursor.execute(f"PRAGMA table_info({self._quote_identifier(table)})").fetchall()
        except sqlite3.Error as exc:
            raise QueryError(f"Failed to fetch SQLite columns for table {table!r}.") from exc
        columns: list[ColumnInfo] = []
        for row in rows:
            native_type = str(row["type"] or "")
            is_primary_key = bool(row["pk"])
            is_auto_increment = is_primary_key and native_type.strip().upper() == "INTEGER"
            columns.append(
                ColumnInfo(
                    name=str(row["name"]),
                    native_type=native_type or "unknown",
                    canonical_type=self._map_sqlite_type(native_type),
                    ordinal=int(row["cid"]) + 1,
                    is_nullable=not bool(row["notnull"]),
                    is_primary_key=is_primary_key,
                    default_value=None if row["dflt_value"] is None else str(row["dflt_value"]),
                    is_auto_increment=is_auto_increment,
                    comment=None,
                )
            )
        return columns

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        cursor = self._cursor()
        try:
            rows = cursor.execute(
                f"PRAGMA foreign_key_list({self._quote_identifier(table)})"
            ).fetchall()
        except sqlite3.Error as exc:
            raise QueryError(f"Failed to fetch SQLite foreign keys for table {table!r}.") from exc
        grouped: dict[int, dict[str, Any]] = defaultdict(
            lambda: {
                "target_table": "",
                "source_columns": [],
                "target_columns": [],
                "on_delete": "NO ACTION",
                "on_update": "NO ACTION",
            }
        )
        for row in rows:
            key = int(row["id"])
            bucket = grouped[key]
            bucket["target_table"] = str(row["table"])
            bucket["on_delete"] = str(row["on_delete"] or "NO ACTION")
            bucket["on_update"] = str(row["on_update"] or "NO ACTION")
            bucket["source_columns"].append(str(row["from"]))
            if row["to"] is not None:
                bucket["target_columns"].append(str(row["to"]))
        foreign_keys: list[ForeignKeyInfo] = []
        for key, payload in grouped.items():
            foreign_keys.append(
                ForeignKeyInfo(
                    name=f"fk_{table}_{key}",
                    source_schema=schema,
                    source_table=table,
                    source_columns=list(payload["source_columns"]),
                    target_schema=schema,
                    target_table=str(payload["target_table"]),
                    target_columns=list(payload["target_columns"]),
                    on_delete=str(payload["on_delete"]),
                    on_update=str(payload["on_update"]),
                )
            )
        return foreign_keys

    def get_indexes(self, schema: str, table: str) -> list[IndexInfo]:
        cursor = self._cursor()
        try:
            rows = cursor.execute(f"PRAGMA index_list({self._quote_identifier(table)})").fetchall()
        except sqlite3.Error as exc:
            raise QueryError(f"Failed to fetch SQLite indexes for table {table!r}.") from exc
        indexes: list[IndexInfo] = []
        for row in rows:
            index_name = str(row["name"])
            origin = str(row["origin"] or "")
            if index_name.startswith("sqlite_") and origin == "c":
                continue
            detail_rows = cursor.execute(
                f"PRAGMA index_info({self._quote_identifier(index_name)})"
            ).fetchall()
            indexes.append(
                IndexInfo(
                    name=index_name,
                    table=table,
                    schema=schema,
                    columns=[str(detail["name"]) for detail in detail_rows],
                    is_unique=bool(row["unique"]),
                    is_primary=origin == "pk",
                    is_partial=bool(row["partial"]),
                    index_type="btree",
                )
            )
        return indexes

    def get_sample_rows(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        privacy_mode: PrivacyMode | None = None,
    ) -> list[dict[str, Any]]:
        effective_mode = self._check_sample_allowed(privacy_mode)
        selected = (
            "*"
            if not columns
            else ", ".join(self._quote_identifier(column) for column in columns)
        )
        effective_limit = limit or self._config.sample_limit
        cursor = self._cursor()
        try:
            rows = cursor.execute(
                f"SELECT {selected} FROM {self._qualified_name(schema, table)} LIMIT ?",
                (int(effective_limit),),
            ).fetchall()
        except sqlite3.Error as exc:
            raise QueryError(f"Failed to sample SQLite table {table!r}.") from exc
        return [self._mask_row(dict(row), effective_mode) for row in rows]

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        cursor = self._cursor()
        try:
            row = cursor.execute(
                f"SELECT COUNT(*) - COUNT({self._quote_identifier(column)}) AS count "
                f"FROM {self._qualified_name(schema, table)}"
            ).fetchone()
        except sqlite3.Error as exc:
            raise QueryError(f"Failed to count NULLs for {table}.{column}.") from exc
        return int(row["count"]) if row is not None else 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        cursor = self._cursor()
        try:
            row = cursor.execute(
                f"SELECT COUNT(DISTINCT {self._quote_identifier(column)}) AS count "
                f"FROM {self._qualified_name(schema, table)}"
            ).fetchone()
        except sqlite3.Error as exc:
            raise QueryError(f"Failed to estimate distinct values for {table}.{column}.") from exc
        return int(row["count"]) if row is not None else 0

    def introspect_schema(self, schema_name: str) -> SchemaInfo:
        schema = super().introspect_schema(schema_name)
        if self._config.database != ":memory:" and self._database_path.exists():
            schema.total_size_bytes = os.path.getsize(self._database_path)
        return schema

    def introspect_all(self) -> IntrospectionResult:
        result = super().introspect_all()
        if self._config.database != ":memory:" and self._database_path.exists():
            for schema in result.schemas:
                if schema.name == "main":
                    schema.total_size_bytes = os.path.getsize(self._database_path)
            result._compute_summary()
        return result
