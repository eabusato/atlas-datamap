"""SQL Server connector for Atlas Datamap."""

from __future__ import annotations

import logging
import os
import queue
import re
import threading
from collections.abc import Generator
from contextlib import contextmanager, suppress
from typing import Any

from atlas.config import AtlasConnectionConfig, PrivacyMode
from atlas.connectors.base import BaseConnector, ConnectionError, QueryError
from atlas.connectors.type_mapping import normalize_type
from atlas.types import ColumnInfo, ForeignKeyInfo, IndexInfo, SchemaInfo, TableInfo, TableType

logger = logging.getLogger(__name__)

SYSTEM_SCHEMAS = frozenset(
    {
        "sys",
        "INFORMATION_SCHEMA",
        "db_owner",
        "db_accessadmin",
        "db_securityadmin",
        "db_ddladmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_denydatareader",
        "db_denydatawriter",
    }
)
TABLESAMPLE_THRESHOLD = 10_000
_DEFAULT_POOL_SIZE = 5
_FREETDS_DRIVER_PATH = "/opt/homebrew/lib/libtdsodbc.so"


def _require_pyodbc() -> Any:
    """Import pyodbc lazily so Atlas still imports without the optional driver."""
    try:
        import pyodbc  # type: ignore[import-not-found]

        return pyodbc
    except ImportError as exc:  # pragma: no cover - exercised when pyodbc is absent
        raise ImportError(
            "pyodbc is not installed. Run 'pip install \"atlas-datamap[mssql]\"' or install 'pyodbc'."
        ) from exc


class _MSSQLConnectionPool:
    """Simple queue-backed pyodbc connection pool."""

    def __init__(self, connection_string: str, pool_size: int = _DEFAULT_POOL_SIZE) -> None:
        self._connection_string = connection_string
        self._pool_size = pool_size
        self._pool: queue.Queue[Any] = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._created = 0
        self._pyodbc = _require_pyodbc()

    def _create_connection(self) -> Any:
        return self._pyodbc.connect(self._connection_string, autocommit=False)

    def get_connection(self) -> Any:
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            pass

        with self._lock:
            if self._created < self._pool_size:
                connection = self._create_connection()
                self._created += 1
                return connection

        try:
            return self._pool.get(timeout=30)
        except queue.Empty as exc:
            raise ConnectionError(
                f"SQL Server connection pool exhausted ({self._pool_size} connections in use)."
            ) from exc

    def return_connection(self, connection: Any) -> None:
        try:
            self._pool.put_nowait(connection)
        except queue.Full:
            with suppress(Exception):
                connection.close()

    def close_all(self) -> None:
        while not self._pool.empty():
            connection = self._pool.get_nowait()
            with suppress(Exception):
                connection.close()
        self._created = 0


def _compose_mssql_native_type(
    type_name: str | None,
    max_length: int | None,
    precision: int | None,
    scale: int | None,
) -> str:
    """Build a readable SQL Server native type string from catalog metadata."""
    if type_name is None:
        return "unknown"
    normalized = type_name.lower()
    if normalized in {
        "int",
        "bigint",
        "smallint",
        "tinyint",
        "bit",
        "money",
        "smallmoney",
        "real",
        "date",
        "time",
        "datetime",
        "datetime2",
        "datetimeoffset",
        "smalldatetime",
        "text",
        "ntext",
        "image",
        "uniqueidentifier",
        "xml",
        "sql_variant",
        "hierarchyid",
        "geography",
        "geometry",
        "timestamp",
        "rowversion",
    }:
        return normalized
    if max_length == -1 and normalized in {"varchar", "nvarchar", "varbinary"}:
        return f"{normalized}(max)"
    if normalized in {"nvarchar", "nchar"} and max_length is not None and max_length > 0:
        return f"{normalized}({max_length // 2})"
    if normalized in {"varchar", "char", "binary", "varbinary"} and max_length is not None:
        return f"{normalized}({max_length})"
    if normalized in {"decimal", "numeric"} and precision is not None:
        if scale is not None:
            return f"{normalized}({precision},{scale})"
        return f"{normalized}({precision})"
    if normalized == "float" and precision is not None:
        return f"{normalized}({precision})"
    return normalized


class MSSQLConnector(BaseConnector):
    """Concrete SQL Server connector backed by pyodbc."""

    def __init__(self, config: AtlasConnectionConfig) -> None:
        super().__init__(config)
        self._pool: _MSSQLConnectionPool | None = None
        self._server_version = ""
        self._server_version_info: tuple[int, int, int] = (0, 0, 0)
        self._edition = ""

    def _candidate_driver(self) -> str:
        configured = self._config.connect_args.get("driver")
        if configured:
            return str(configured)
        if os.path.exists(_FREETDS_DRIVER_PATH):
            return _FREETDS_DRIVER_PATH
        return "ODBC Driver 18 for SQL Server"

    def _build_connection_string(self, *, database: str | None = None) -> str:
        driver = self._candidate_driver()
        database_name = database or self._config.database
        parts = [f"DRIVER={driver}", f"SERVER={self._config.host}"]
        if self._config.port is not None:
            parts.append(f"PORT={self._config.port}")
        parts.append(f"DATABASE={database_name}")
        if self._config.user:
            parts.append(f"UID={self._config.user}")
            parts.append(f"PWD={self._config.password or ''}")
        else:
            parts.append("Trusted_Connection=yes")
        if driver == _FREETDS_DRIVER_PATH:
            parts.append("TDS_Version=7.4")
            parts.append("ClientCharset=UTF-8")
        ssl_mode = self._config.ssl_mode
        if ssl_mode == "disable":
            parts.append("Encrypt=no")
        elif ssl_mode in {"verify-ca", "verify-full"}:
            parts.append("Encrypt=yes")
            parts.append("TrustServerCertificate=no")
        else:
            parts.append("Encrypt=yes")
            parts.append("TrustServerCertificate=yes")
        parts.append(f"Connection Timeout={self._config.timeout_seconds}")
        return ";".join(parts)

    def connect(self) -> None:
        """Create the SQL Server connection pool and detect the server version."""
        if self._connected and self._pool is not None:
            return
        try:
            self._pool = _MSSQLConnectionPool(
                self._build_connection_string(),
                pool_size=int(self._config.connect_args.get("pool_size", _DEFAULT_POOL_SIZE)),
            )
            connection = self._pool.get_connection()
            self._pool.return_connection(connection)
        except Exception as exc:  # pragma: no cover - exercised by unit tests with mocks
            raise ConnectionError(
                f"Failed to connect to {self._config.connection_string_safe}: {exc}"
            ) from exc
        self._connected = True
        self._server_version, self._server_version_info, self._edition = self._detect_server_version()

    def disconnect(self) -> None:
        """Close the queue-backed pool and clear the connected flag."""
        if self._pool is not None:
            self._pool.close_all()
            self._pool = None
        self._connected = False

    def ping(self) -> bool:
        """Check whether the server responds to a lightweight SELECT."""
        if not self._connected or self._pool is None:
            return False
        try:
            with self._cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except QueryError:
            return False

    def get_server_version(self) -> str:
        """Return the cached SQL Server version string."""
        return self._server_version

    @property
    def server_version_info(self) -> tuple[int, int, int]:
        """Return the parsed server version tuple."""
        return self._server_version_info

    @property
    def edition(self) -> str:
        """Return the detected SQL Server edition name when available."""
        return self._edition

    @contextmanager
    def _cursor(self) -> Generator[Any, None, None]:
        """Borrow a connection, apply read-oriented session settings, and yield a cursor."""
        if self._pool is None:
            raise ConnectionError("SQL Server connector is not connected. Call connect() first.")
        connection = self._pool.get_connection()
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            cursor.execute("SET LOCK_TIMEOUT 5000")
            yield cursor
        except QueryError:
            raise
        except Exception as exc:
            raise QueryError(f"SQL Server query failed: {exc}") from exc
        finally:
            with suppress(Exception):
                connection.rollback()
            if cursor is not None:
                with suppress(Exception):
                    cursor.close()
            self._pool.return_connection(connection)

    def _detect_server_version(self) -> tuple[str, tuple[int, int, int], str]:
        with self._cursor() as cursor:
            cursor.execute("SELECT @@VERSION")
            row = cursor.fetchone()
        version_text = str(row[0]) if row else ""
        version_match = re.search(r"(\d+)\.(\d+)\.(\d+)", version_text)
        edition_match = re.search(
            r"(Developer|Enterprise|Standard|Express|Web|Azure) Edition", version_text
        )
        version_info = (
            (
                int(version_match.group(1)),
                int(version_match.group(2)),
                int(version_match.group(3)),
            )
            if version_match
            else (0, 0, 0)
        )
        edition = edition_match.group(1) if edition_match else ""
        return version_text, version_info, edition

    def _quote_identifier(self, identifier: str) -> str:
        return "[" + identifier.replace("]", "]]") + "]"

    def _qualified_name(self, schema: str, table: str) -> str:
        return f"{self._quote_identifier(schema)}.{self._quote_identifier(table)}"

    def _fetchall(self, sql: str, params: tuple[Any, ...]) -> list[Any]:
        with self._cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return list(cursor.fetchall())

    def _fetchone(self, sql: str, params: tuple[Any, ...]) -> Any | None:
        with self._cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchone()

    def get_schemas(self) -> list[SchemaInfo]:
        sql = """
            SELECT s.name
            FROM sys.schemas s
            WHERE s.name NOT IN (
                'sys',
                'INFORMATION_SCHEMA',
                'db_owner',
                'db_accessadmin',
                'db_securityadmin',
                'db_ddladmin',
                'db_backupoperator',
                'db_datareader',
                'db_datawriter',
                'db_denydatareader',
                'db_denydatawriter'
            )
            ORDER BY s.name
        """
        rows = self._fetchall(sql, ())
        schemas: list[SchemaInfo] = []
        for (schema_name,) in rows:
            normalized_schema = str(schema_name)
            if normalized_schema in SYSTEM_SCHEMAS:
                continue
            if self._should_include_schema(normalized_schema):
                schemas.append(
                    SchemaInfo(name=normalized_schema, engine=self._config.engine.value)
                )
        return schemas

    def get_tables(self, schema: str) -> list[TableInfo]:
        sql = """
            SELECT
                obj.name AS table_name,
                obj.type AS object_type,
                CAST(ep.value AS NVARCHAR(MAX)) AS table_comment
            FROM (
                SELECT name, object_id, schema_id, type FROM sys.tables
                UNION ALL
                SELECT name, object_id, schema_id, type FROM sys.views
                UNION ALL
                SELECT name, object_id, schema_id, type FROM sys.synonyms
            ) obj
            JOIN sys.schemas s
              ON s.schema_id = obj.schema_id
            LEFT JOIN sys.extended_properties ep
              ON ep.major_id = obj.object_id
             AND ep.minor_id = 0
             AND ep.class = 1
             AND ep.name = 'MS_Description'
            WHERE s.name = ?
            ORDER BY obj.name
        """
        tables: list[TableInfo] = []
        for name, object_type, comment in self._fetchall(sql, (schema,)):
            type_code = str(object_type or "").strip().upper()
            if type_code == "V":
                table_type = TableType.VIEW
            elif type_code == "SN":
                table_type = TableType.SYNONYM
            else:
                table_type = TableType.TABLE
            tables.append(
                TableInfo(
                    name=str(name),
                    schema=schema,
                    table_type=table_type,
                    comment=str(comment) if comment is not None else None,
                )
            )
        return tables

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        sql = """
            SELECT SUM(ps.row_count)
            FROM sys.dm_db_partition_stats ps
            JOIN sys.objects o ON o.object_id = ps.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ?
              AND o.name = ?
              AND ps.index_id IN (0, 1)
        """
        row = self._fetchone(sql, (schema, table))
        return int(row[0]) if row and row[0] is not None else 0

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        sql = """
            SELECT SUM(au.total_pages) * 8 * 1024
            FROM sys.allocation_units au
            JOIN sys.partitions p ON p.partition_id = au.container_id
            JOIN sys.objects o ON o.object_id = p.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ?
              AND o.name = ?
        """
        row = self._fetchone(sql, (schema, table))
        return int(row[0]) if row and row[0] is not None else 0

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        sql = """
            SELECT
                c.name AS column_name,
                tp.name AS type_name,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                dc.definition AS column_default,
                pk_cols.column_id AS pk_column_id,
                CAST(ep.value AS NVARCHAR(MAX)) AS column_comment,
                c.column_id
            FROM sys.columns c
            JOIN sys.objects o ON o.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            JOIN sys.types tp ON tp.user_type_id = c.user_type_id
            LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
            LEFT JOIN (
                SELECT ic.object_id, ic.column_id
                FROM sys.index_columns ic
                JOIN sys.indexes i
                  ON i.object_id = ic.object_id
                 AND i.index_id = ic.index_id
                WHERE i.is_primary_key = 1
            ) pk_cols
              ON pk_cols.object_id = c.object_id
             AND pk_cols.column_id = c.column_id
            LEFT JOIN sys.extended_properties ep
              ON ep.major_id = c.object_id
             AND ep.minor_id = c.column_id
             AND ep.class = 1
             AND ep.name = 'MS_Description'
            WHERE s.name = ?
              AND o.name = ?
            ORDER BY c.column_id
        """
        columns: list[ColumnInfo] = []
        for row in self._fetchall(sql, (schema, table)):
            (
                column_name,
                type_name,
                max_length,
                precision,
                scale,
                is_nullable,
                is_identity,
                column_default,
                pk_column_id,
                comment,
                column_id,
            ) = row
            native_type = _compose_mssql_native_type(
                str(type_name) if type_name is not None else None,
                int(max_length) if max_length is not None else None,
                int(precision) if precision is not None else None,
                int(scale) if scale is not None else None,
            )
            columns.append(
                ColumnInfo(
                    name=str(column_name),
                    native_type=native_type,
                    canonical_type=normalize_type(native_type, "mssql"),
                    ordinal=int(column_id or 0),
                    is_nullable=bool(is_nullable),
                    is_primary_key=pk_column_id is not None,
                    is_auto_increment=bool(is_identity),
                    default_value=str(column_default) if column_default is not None else None,
                    comment=str(comment) if comment is not None else None,
                )
            )
        return columns

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        sql = """
            SELECT
                fk.name AS constraint_name,
                fkc.constraint_column_id,
                src_col.name AS source_column,
                ref_sch.name AS ref_schema,
                ref_obj.name AS ref_table,
                ref_col.name AS ref_column,
                fk.delete_referential_action_desc AS on_delete,
                fk.update_referential_action_desc AS on_update
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc
              ON fkc.constraint_object_id = fk.object_id
            JOIN sys.objects src_obj
              ON src_obj.object_id = fk.parent_object_id
            JOIN sys.schemas src_sch
              ON src_sch.schema_id = src_obj.schema_id
            JOIN sys.columns src_col
              ON src_col.object_id = fk.parent_object_id
             AND src_col.column_id = fkc.parent_column_id
            JOIN sys.objects ref_obj
              ON ref_obj.object_id = fk.referenced_object_id
            JOIN sys.schemas ref_sch
              ON ref_sch.schema_id = ref_obj.schema_id
            JOIN sys.columns ref_col
              ON ref_col.object_id = fk.referenced_object_id
             AND ref_col.column_id = fkc.referenced_column_id
            WHERE src_sch.name = ?
              AND src_obj.name = ?
            ORDER BY fk.name, fkc.constraint_column_id
        """
        grouped: dict[str, ForeignKeyInfo] = {}
        for row in self._fetchall(sql, (schema, table)):
            (
                constraint_name,
                _constraint_column_id,
                source_column,
                referenced_schema,
                referenced_table,
                referenced_column,
                on_delete,
                on_update,
            ) = row
            name = str(constraint_name)
            if name not in grouped:
                grouped[name] = ForeignKeyInfo(
                    name=name,
                    source_schema=schema,
                    source_table=table,
                    source_columns=[],
                    target_schema=str(referenced_schema),
                    target_table=str(referenced_table),
                    target_columns=[],
                    on_delete=str(on_delete or "NO ACTION").replace("_", " "),
                    on_update=str(on_update or "NO ACTION").replace("_", " "),
                    is_inferred=False,
                )
            grouped[name].source_columns.append(str(source_column))
            grouped[name].target_columns.append(str(referenced_column))
        return list(grouped.values())

    def get_indexes(self, schema: str, table: str) -> list[IndexInfo]:
        sql = """
            SELECT
                i.name AS index_name,
                i.type_desc AS index_type,
                i.is_unique,
                i.is_primary_key,
                i.has_filter,
                c.name AS column_name,
                ic.key_ordinal
            FROM sys.indexes i
            JOIN sys.index_columns ic
              ON ic.object_id = i.object_id
             AND ic.index_id = i.index_id
            JOIN sys.columns c
              ON c.object_id = i.object_id
             AND c.column_id = ic.column_id
            JOIN sys.objects o
              ON o.object_id = i.object_id
            JOIN sys.schemas s
              ON s.schema_id = o.schema_id
            WHERE s.name = ?
              AND o.name = ?
              AND i.type != 0
              AND ic.is_included_column = 0
            ORDER BY i.name, ic.key_ordinal
        """
        grouped: dict[str, IndexInfo] = {}
        for row in self._fetchall(sql, (schema, table)):
            index_name, index_type, is_unique, is_primary, has_filter, column_name, _ordinal = row
            name = str(index_name)
            if name not in grouped:
                grouped[name] = IndexInfo(
                    name=name,
                    table=table,
                    schema=schema,
                    columns=[],
                    is_unique=bool(is_unique),
                    is_primary=bool(is_primary),
                    is_partial=bool(has_filter),
                    index_type=str(index_type or "NONCLUSTERED").lower().replace(" ", "_"),
                )
            grouped[name].columns.append(str(column_name))
        return list(grouped.values())

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        """Return zero when SQL Server catalog metadata does not expose a cheap null-count estimate."""
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        """Return zero when SQL Server catalog metadata does not expose a cheap distinct estimate."""
        return 0

    def get_sample_rows(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        privacy_mode: PrivacyMode | None = None,
    ) -> list[dict[str, Any]]:
        effective_mode = self._check_sample_allowed(privacy_mode)
        effective_limit = self._config.sample_limit if limit is None else limit
        if effective_limit <= 0:
            return []
        selected_columns = (
            "*"
            if not columns
            else ", ".join(self._quote_identifier(column_name) for column_name in columns)
        )
        qualified_name = self._qualified_name(schema, table)
        row_estimate = self.get_row_count_estimate(schema, table)

        if row_estimate >= TABLESAMPLE_THRESHOLD:
            sql = f"SELECT TOP {effective_limit} {selected_columns} FROM {qualified_name} TABLESAMPLE ({effective_limit} ROWS)"
            params: tuple[Any, ...] = ()
        else:
            sql = f"SELECT TOP {effective_limit} {selected_columns} FROM {qualified_name} ORDER BY NEWID()"
            params = ()

        with self._cursor() as cursor:
            cursor.execute(sql, params) if params else cursor.execute(sql)
            if cursor.description is None:
                return []
            column_names = [description[0] for description in cursor.description]
            rows = cursor.fetchall()

        if not rows and row_estimate >= TABLESAMPLE_THRESHOLD:
            with self._cursor() as cursor:
                cursor.execute(
                    f"SELECT TOP {effective_limit} {selected_columns} FROM {qualified_name} ORDER BY NEWID()"
                )
                if cursor.description is None:
                    return []
                column_names = [description[0] for description in cursor.description]
                rows = cursor.fetchall()

        return [
            self._mask_row(dict(zip(column_names, row, strict=False)), effective_mode) for row in rows
        ]

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        version_suffix = f" [{self._server_version}]" if self._server_version else ""
        return f"MSSQLConnector({self._config.connection_string_safe}{version_suffix}, {status})"
