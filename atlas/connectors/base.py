"""Abstract database connector contract for Atlas."""

from __future__ import annotations

import abc
import datetime
import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from atlas.config import AtlasConnectionConfig, PrivacyMode
from atlas.types import (
    ColumnInfo,
    ColumnStats,
    ForeignKeyInfo,
    IndexInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
)

logger = logging.getLogger(__name__)


class ConnectorError(RuntimeError):
    """Base connector error that wraps engine-specific failures."""


class ConnectionError(ConnectorError):  # noqa: A001
    """Raised when a connector cannot establish a connection."""


class QueryError(ConnectorError):
    """Raised when a metadata or stats query fails."""


class PrivacyViolationError(ConnectorError):
    """Raised when data sampling is not allowed for the active privacy mode."""


class BaseConnector(abc.ABC):
    """Abstract contract shared by all Atlas connectors."""

    def __init__(self, config: AtlasConnectionConfig) -> None:
        self._config = config
        self._connected = False
        self._logger = logging.getLogger(f"atlas.connectors.{config.engine.value}")

    @property
    def config(self) -> AtlasConnectionConfig:
        return self._config

    @property
    def is_connected(self) -> bool:
        return self._connected

    @abc.abstractmethod
    def connect(self) -> None:
        """Establish a connection and set the connector as connected."""

    @abc.abstractmethod
    def disconnect(self) -> None:
        """Close the connection and clear the connected flag."""

    def ping(self) -> bool:
        if self._connected:
            return True
        try:
            self.connect()
        except ConnectorError:
            return False
        return True

    @contextmanager
    def session(self) -> Generator[None, None, None]:
        self.connect()
        try:
            yield
        finally:
            self.disconnect()

    @abc.abstractmethod
    def get_schemas(self) -> list[SchemaInfo]:
        """Return available schemas after applying config filters."""

    def _should_include_schema(self, schema_name: str) -> bool:
        if schema_name in self._config.schema_exclude:
            return False
        if self._config.schema_filter:
            return schema_name in self._config.schema_filter
        return True

    @abc.abstractmethod
    def get_tables(self, schema: str) -> list[TableInfo]:
        """Return tables and views for a schema."""

    @abc.abstractmethod
    def get_row_count_estimate(self, schema: str, table: str) -> int:
        """Return a fast row-count estimate."""

    @abc.abstractmethod
    def get_table_size_bytes(self, schema: str, table: str) -> int:
        """Return physical table size in bytes when available."""

    @abc.abstractmethod
    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        """Return ordered column metadata."""

    @abc.abstractmethod
    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        """Return declared foreign keys for a table."""

    @abc.abstractmethod
    def get_indexes(self, schema: str, table: str) -> list[IndexInfo]:
        """Return table indexes."""

    @abc.abstractmethod
    def get_sample_rows(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        privacy_mode: PrivacyMode | None = None,
    ) -> list[dict[str, Any]]:
        """Return privacy-aware sample rows."""

    def _check_sample_allowed(self, privacy_mode: PrivacyMode | None = None) -> PrivacyMode:
        effective_mode = privacy_mode or self._config.privacy_mode
        if not effective_mode.allows_samples:
            raise PrivacyViolationError(
                "Sample access is not allowed in privacy mode "
                f"{effective_mode.value!r}. Use 'normal' or 'masked'."
            )
        return effective_mode

    def _mask_row(self, row: dict[str, Any], privacy_mode: PrivacyMode) -> dict[str, Any]:
        if privacy_mode is PrivacyMode.normal:
            return {
                column: (str(value) if value is not None else None) for column, value in row.items()
            }
        masked: dict[str, Any] = {}
        for column, value in row.items():
            if self._config.is_column_sensitive(column):
                masked[column] = "***"
            else:
                masked[column] = str(value) if value is not None else None
        return masked

    def get_column_stats(self, schema: str, table: str, column: str) -> ColumnStats:
        row_count = self.get_row_count_estimate(schema, table)
        null_count = self.get_column_null_count(schema, table, column)
        distinct_count = self.get_column_distinct_estimate(schema, table, column)
        return ColumnStats(
            row_count=row_count,
            null_count=null_count,
            distinct_count=distinct_count,
        )

    @abc.abstractmethod
    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        """Return the number of NULL values in a column."""

    @abc.abstractmethod
    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        """Return an estimated distinct count for a column."""

    def introspect_schema(self, schema_name: str) -> SchemaInfo:
        self._logger.info("Introspecting schema %s", schema_name)
        tables = self.get_tables(schema_name)
        total_size_bytes = 0

        for table in tables:
            table.columns = self.get_columns(schema_name, table.name)
            table.foreign_keys = self.get_foreign_keys(schema_name, table.name)
            table.indexes = self.get_indexes(schema_name, table.name)
            table.column_count = len(table.columns)
            table.row_count_estimate = self.get_row_count_estimate(schema_name, table.name)
            table.size_bytes = self.get_table_size_bytes(schema_name, table.name)
            total_size_bytes += table.size_bytes

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

        return SchemaInfo(
            name=schema_name,
            engine=self._config.engine.value,
            tables=tables,
            total_size_bytes=total_size_bytes,
            introspected_at=datetime.datetime.now(datetime.UTC).isoformat(),
        )

    def introspect_all(self) -> IntrospectionResult:
        self._logger.info("Starting full introspection for %s", self._config.connection_string_safe)
        schemas = [self.introspect_schema(schema.name) for schema in self.get_schemas()]

        fk_in_degree_map: dict[str, list[str]] = {}
        for schema in schemas:
            for table in schema.tables:
                source_qualified_name = table.qualified_name
                for foreign_key in table.foreign_keys:
                    target = f"{foreign_key.target_schema}.{foreign_key.target_table}"
                    fk_in_degree_map.setdefault(target, [])
                    if source_qualified_name not in fk_in_degree_map[target]:
                        fk_in_degree_map[target].append(source_qualified_name)

        return IntrospectionResult(
            database=self._config.database,
            engine=self._config.engine.value,
            host=self._config.host,
            schemas=schemas,
            fk_in_degree_map=fk_in_degree_map,
            introspected_at=datetime.datetime.now(datetime.UTC).isoformat(),
        )

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        return f"{self.__class__.__name__}({self._config.connection_string_safe}, {status})"
