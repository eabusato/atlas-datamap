"""Generic SQLAlchemy-backed connector for non-native dialects."""

from __future__ import annotations

import importlib
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, cast

from atlas.config import AtlasConnectionConfig, PrivacyMode
from atlas.connectors.base import BaseConnector, ConnectionError
from atlas.types import (
    AtlasType,
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    SchemaInfo,
    TableInfo,
    TableType,
)


def _require_sqlalchemy() -> tuple[Any, Any, Any, Any]:
    try:
        sqlalchemy = importlib.import_module("sqlalchemy")
        sqlalchemy_exc = importlib.import_module("sqlalchemy.exc")
    except ImportError as exc:  # pragma: no cover - depends on environment
        raise ImportError(
            "SQLAlchemy support is not installed. Run 'pip install \"atlas-datamap[generic]\"' "
            "or install 'sqlalchemy'."
        ) from exc
    return (
        sqlalchemy.create_engine,
        sqlalchemy.inspect,
        sqlalchemy.text,
        sqlalchemy_exc.SQLAlchemyError,
    )


class SQLAlchemyConnector(BaseConnector):
    """Degraded connector that relies on SQLAlchemy inspection only."""

    def __init__(self, config: AtlasConnectionConfig) -> None:
        super().__init__(config)
        self._engine: Any | None = None
        self._sqlalchemy_url = str(self._config.connect_args.get("sqlalchemy_url", "")).strip()
        self._degraded_warning_emitted = False
        if not self._sqlalchemy_url:
            raise ConnectionError(
                "Generic connector requires 'connect_args[\"sqlalchemy_url\"]' in AtlasConnectionConfig."
            )

    def connect(self) -> None:
        if self._connected:
            return
        create_engine, _inspect, _text, sqlalchemy_error = _require_sqlalchemy()
        try:
            self._engine = create_engine(self._sqlalchemy_url)
            with self._engine.connect() as connection:
                connection.exec_driver_sql("SELECT 1")
        except sqlalchemy_error as exc:
            raise ConnectionError(
                f"Failed to connect using generic SQLAlchemy URL {self._config.connection_string_safe}: {exc}"
            ) from exc
        self._connected = True
        if not self._degraded_warning_emitted:
            self._logger.warning(
                "Generic SQLAlchemy connector active for %s. Row counts, sizes, and column "
                "statistics are unavailable. Sigilo will use uniform node sizing until a "
                "native Atlas connector is available for this dialect.",
                self._config.connection_string_safe,
            )
            self._degraded_warning_emitted = True

    def disconnect(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._connected = False

    @contextmanager
    def _inspector(self) -> Generator[Any, None, None]:
        if self._engine is None:
            raise ConnectionError("Generic SQLAlchemy connector is not connected.")
        _create_engine, inspect, _text, _sqlalchemy_error = _require_sqlalchemy()
        yield inspect(self._engine)

    def _quote_identifier(self, identifier: str) -> str:
        if self._engine is None:
            raise ConnectionError("Generic SQLAlchemy connector is not connected.")
        preparer = self._engine.dialect.identifier_preparer
        return cast(str, preparer.quote_identifier(identifier))

    def _qualified_name(self, schema: str, table: str) -> str:
        quoted_table = self._quote_identifier(table)
        if schema and schema not in {"main", "default"}:
            return f"{self._quote_identifier(schema)}.{quoted_table}"
        return quoted_table

    @staticmethod
    def _map_sqla_type(type_value: Any) -> AtlasType:
        normalized = str(type_value).strip().lower()
        if not normalized:
            return AtlasType.UNKNOWN
        if "uuid" in normalized:
            return AtlasType.UUID
        if "json" in normalized:
            return AtlasType.JSON
        if "bool" in normalized:
            return AtlasType.BOOLEAN
        if "timestamp" in normalized:
            return AtlasType.TIMESTAMP
        if "datetime" in normalized:
            return AtlasType.DATETIME
        if normalized == "date":
            return AtlasType.DATE
        if normalized == "time":
            return AtlasType.TIME
        if "bigint" in normalized:
            return AtlasType.BIGINT
        if "smallint" in normalized:
            return AtlasType.SMALLINT
        if "tinyint" in normalized:
            return AtlasType.TINYINT
        if "int" in normalized:
            return AtlasType.INTEGER
        if "double" in normalized:
            return AtlasType.DOUBLE
        if "float" in normalized or "real" in normalized:
            return AtlasType.FLOAT
        if "decimal" in normalized or "numeric" in normalized:
            return AtlasType.DECIMAL
        if "char" in normalized and "var" not in normalized:
            return AtlasType.CHAR
        if "text" in normalized or "clob" in normalized:
            return AtlasType.CLOB
        if "char" in normalized or "string" in normalized or "varchar" in normalized:
            return AtlasType.TEXT
        if "blob" in normalized or "binary" in normalized or "bytea" in normalized:
            return AtlasType.BINARY
        return AtlasType.UNKNOWN

    def get_schemas(self) -> list[SchemaInfo]:
        with self._inspector() as inspector:
            try:
                schema_names = list(inspector.get_schema_names())
            except NotImplementedError:
                schema_names = []
        if not schema_names:
            schema_names = ["main"]
        ignore = {"information_schema", "pg_catalog", "sys", "sysibm"}
        schemas: list[SchemaInfo] = []
        for schema_name in schema_names:
            normalized = str(schema_name or "main")
            if normalized.lower() in ignore:
                continue
            if self._should_include_schema(normalized):
                schemas.append(SchemaInfo(name=normalized, engine="generic"))
        return schemas

    def get_tables(self, schema: str) -> list[TableInfo]:
        with self._inspector() as inspector:
            try:
                table_names = inspector.get_table_names(schema=schema)
            except NotImplementedError:
                table_names = inspector.get_table_names()
            try:
                view_names = inspector.get_view_names(schema=schema)
            except NotImplementedError:
                view_names = []
            try:
                materialized_views = inspector.get_materialized_view_names(schema=schema)
            except Exception:
                materialized_views = []
        tables: list[TableInfo] = []
        for name in table_names:
            tables.append(TableInfo(name=str(name), schema=schema, table_type=TableType.TABLE))
        for name in view_names:
            tables.append(TableInfo(name=str(name), schema=schema, table_type=TableType.VIEW))
        for name in materialized_views:
            tables.append(
                TableInfo(name=str(name), schema=schema, table_type=TableType.MATERIALIZED_VIEW)
            )
        return sorted(tables, key=lambda item: (item.table_type.value, item.name))

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        return 0

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        return 0

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        with self._inspector() as inspector:
            columns_info = inspector.get_columns(table, schema=schema)
            pk_constraint = inspector.get_pk_constraint(table, schema=schema) or {}
        pk_columns = set(pk_constraint.get("constrained_columns") or [])
        columns: list[ColumnInfo] = []
        for ordinal, column in enumerate(columns_info, start=1):
            native_type = str(column.get("type") or "unknown")
            columns.append(
                ColumnInfo(
                    name=str(column["name"]),
                    native_type=native_type,
                    canonical_type=self._map_sqla_type(column.get("type")),
                    ordinal=ordinal,
                    is_nullable=bool(column.get("nullable", True)),
                    is_primary_key=str(column["name"]) in pk_columns,
                    default_value=(
                        None if column.get("default") is None else str(column.get("default"))
                    ),
                    comment=(
                        None if column.get("comment") is None else str(column.get("comment"))
                    ),
                )
            )
        return columns

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        with self._inspector() as inspector:
            raw_keys = inspector.get_foreign_keys(table, schema=schema)
        foreign_keys: list[ForeignKeyInfo] = []
        for index, item in enumerate(raw_keys):
            foreign_keys.append(
                ForeignKeyInfo(
                    name=str(item.get("name") or f"fk_{table}_{index}"),
                    source_schema=schema,
                    source_table=table,
                    source_columns=[str(value) for value in item.get("constrained_columns") or []],
                    target_schema=str(item.get("referred_schema") or schema),
                    target_table=str(item.get("referred_table") or ""),
                    target_columns=[str(value) for value in item.get("referred_columns") or []],
                )
            )
        return foreign_keys

    def get_indexes(self, schema: str, table: str) -> list[IndexInfo]:
        with self._inspector() as inspector:
            raw_indexes = inspector.get_indexes(table, schema=schema)
            try:
                pk_constraint = inspector.get_pk_constraint(table, schema=schema) or {}
            except Exception:
                pk_constraint = {}
        indexes: list[IndexInfo] = []
        pk_columns = [str(value) for value in pk_constraint.get("constrained_columns") or []]
        if pk_columns:
            indexes.append(
                IndexInfo(
                    name=str(pk_constraint.get("name") or f"{table}_pk"),
                    table=table,
                    schema=schema,
                    columns=pk_columns,
                    is_unique=True,
                    is_primary=True,
                )
            )
        for item in raw_indexes:
            indexes.append(
                IndexInfo(
                    name=str(item.get("name") or f"{table}_idx"),
                    table=table,
                    schema=schema,
                    columns=[str(value) for value in item.get("column_names") or []],
                    is_unique=bool(item.get("unique", False)),
                    is_primary=False,
                    is_partial=item.get("dialect_options", {}).get("sqlite_where") is not None,
                    index_type=str(item.get("type") or "btree"),
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
        _create_engine, _inspect, text, sqlalchemy_error = _require_sqlalchemy()
        if self._engine is None:
            raise ConnectionError("Generic SQLAlchemy connector is not connected.")
        effective_limit = limit or self._config.sample_limit
        selected = (
            "*"
            if not columns
            else ", ".join(self._quote_identifier(column) for column in columns)
        )
        query = (
            f"SELECT {selected} FROM {self._qualified_name(schema, table)} LIMIT :limit"
        )
        try:
            with self._engine.connect() as connection:
                rows = connection.execute(text(query), {"limit": int(effective_limit)})
                return [
                    self._mask_row(dict(row._mapping), effective_mode)
                    for row in rows
                ]
        except sqlalchemy_error:
            return []

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        return 0
