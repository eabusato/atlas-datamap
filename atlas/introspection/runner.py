"""Structured orchestration for database-wide Atlas introspection."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from atlas.config import AtlasConnectionConfig
from atlas.connectors.base import BaseConnector
from atlas.types import IntrospectionResult, SchemaInfo, TableInfo


@dataclass(slots=True)
class _ProgressEvent:
    """Progress payload emitted during orchestration."""

    stage: str
    message: str
    current: int = 0
    total: int = 0
    elapsed_ms: int = 0


ProgressCallback = Callable[[_ProgressEvent], None]


class IntrospectionError(RuntimeError):
    """Raised when introspection fails for a specific table or stage."""


class IntrospectionRunner:
    """Own the connector lifecycle and build an ``IntrospectionResult`` incrementally."""

    def __init__(
        self,
        config: AtlasConnectionConfig,
        connector: BaseConnector,
        on_progress: ProgressCallback | None = None,
        *,
        skip_columns: bool = False,
        skip_indexes: bool = False,
    ) -> None:
        self._config = config
        self._connector = connector
        self._on_progress = on_progress
        self._skip_columns = skip_columns
        self._skip_indexes = skip_indexes
        self._started_at = 0.0

    def run(self) -> IntrospectionResult:
        """Run full introspection and always close the connector afterwards."""

        self._started_at = time.perf_counter()
        self._emit("connect", f"Connecting to {self._config.connection_string_safe}")
        self._connector.connect()
        try:
            self._emit("schemas", "Loading available schemas")
            discovered_schemas = self._connector.get_schemas()
            schemas = self._filter_schemas(discovered_schemas)
            self._emit("schemas", f"Selected {len(schemas)} schema(s)", len(schemas), len(schemas))

            rendered_schemas: list[SchemaInfo] = []
            for schema_index, schema in enumerate(schemas, start=1):
                rendered_schemas.append(
                    self._introspect_schema(
                        schema.name,
                        current_schema=schema_index,
                        total_schemas=len(schemas),
                    )
                )

            fk_in_degree_map: dict[str, list[str]] = {}
            for schema in rendered_schemas:
                for table in schema.tables:
                    source = table.qualified_name
                    for foreign_key in table.foreign_keys:
                        target = f"{foreign_key.target_schema}.{foreign_key.target_table}"
                        fk_in_degree_map.setdefault(target, [])
                        if source not in fk_in_degree_map[target]:
                            fk_in_degree_map[target].append(source)

            return IntrospectionResult(
                database=self._config.database,
                engine=self._config.engine.value,
                host=self._config.host,
                schemas=rendered_schemas,
                fk_in_degree_map=fk_in_degree_map,
                introspected_at=datetime.now(UTC).isoformat(),
            )
        finally:
            self._connector.disconnect()

    def _filter_schemas(self, schemas: list[SchemaInfo]) -> list[SchemaInfo]:
        filtered: list[SchemaInfo] = []
        for schema in schemas:
            if schema.name in self._config.schema_exclude:
                continue
            if self._config.schema_filter and schema.name not in self._config.schema_filter:
                continue
            filtered.append(schema)
        return filtered

    def _introspect_schema(
        self,
        schema_name: str,
        *,
        current_schema: int,
        total_schemas: int,
    ) -> SchemaInfo:
        self._emit(
            "tables",
            f"Loading tables for schema {schema_name}",
            current_schema,
            total_schemas,
        )
        tables = self._connector.get_tables(schema_name)
        total_size_bytes = 0

        for table_index, table in enumerate(tables, start=1):
            self._introspect_table(
                schema_name,
                table,
                current_table=table_index,
                total_tables=len(tables),
            )
            total_size_bytes += table.size_bytes

        return SchemaInfo(
            name=schema_name,
            engine=self._config.engine.value,
            tables=tables,
            total_size_bytes=total_size_bytes,
            introspected_at=datetime.now(UTC).isoformat(),
        )

    def _introspect_table(
        self,
        schema_name: str,
        table: TableInfo,
        *,
        current_table: int,
        total_tables: int,
    ) -> None:
        qualified_name = f"{schema_name}.{table.name}"
        try:
            if not self._skip_columns:
                self._emit(
                    "columns",
                    f"Loading columns for {qualified_name}",
                    current_table,
                    total_tables,
                )
                table.columns = self._connector.get_columns(schema_name, table.name)
                table.column_count = len(table.columns)

            self._emit(
                "relations",
                f"Loading relations for {qualified_name}",
                current_table,
                total_tables,
            )
            table.foreign_keys = self._connector.get_foreign_keys(schema_name, table.name)
            table.indexes = [] if self._skip_indexes else self._connector.get_indexes(schema_name, table.name)
            table.row_count_estimate = self._connector.get_row_count_estimate(schema_name, table.name)
            table.size_bytes = self._connector.get_table_size_bytes(schema_name, table.name)

            indexed_columns = {column for index in table.indexes for column in index.columns}
            fk_source_columns = {
                column
                for foreign_key in table.foreign_keys
                for column in foreign_key.source_columns
            }
            for column in table.columns:
                if column.name in indexed_columns:
                    column.is_indexed = True
                if column.name in fk_source_columns:
                    column.is_foreign_key = True
            table.refresh_derived_fields()
        except Exception as exc:
            raise IntrospectionError(f"Failed to introspect table {qualified_name}: {exc}") from exc

    def _emit(self, stage: str, message: str, current: int = 0, total: int = 0) -> None:
        if self._on_progress is None:
            return
        elapsed_ms = int((time.perf_counter() - self._started_at) * 1000) if self._started_at else 0
        self._on_progress(
            _ProgressEvent(
                stage=stage,
                message=message,
                current=current,
                total=total,
                elapsed_ms=elapsed_ms,
            )
        )
