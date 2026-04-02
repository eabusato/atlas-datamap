"""PostgreSQL connector for Atlas Datamap."""

from __future__ import annotations

import datetime
import logging
from collections.abc import Generator
from contextlib import contextmanager, suppress
from csv import reader
from io import StringIO
from typing import Any

from atlas.config import AtlasConnectionConfig, PrivacyMode
from atlas.connectors.base import BaseConnector, ConnectionError, QueryError
from atlas.connectors.type_mapping import normalize_type
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

logger = logging.getLogger(__name__)

TABLESAMPLE_THRESHOLD = 10_000
_DEFAULT_POOL_MAX = 4


def _require_psycopg2() -> Any:
    """Import psycopg2 lazily so the package still imports without the driver."""
    try:
        import psycopg2  # type: ignore[import-untyped]
        import psycopg2.pool  # type: ignore[import-untyped]

        return psycopg2
    except ImportError as exc:  # pragma: no cover - exercised in environments without psycopg2
        raise ImportError(
            "psycopg2 is not installed. Run 'pip install \"atlas-datamap[postgresql]\"' "
            "or install 'psycopg2-binary'."
        ) from exc


def _compose_native_type(
    data_type: str,
    udt_name: str,
    char_max_len: int | None,
    num_precision: int | None,
    num_scale: int | None,
) -> str:
    """Build a readable PostgreSQL native type string from information_schema fields."""
    if data_type == "ARRAY":
        internal_to_readable = {
            "int2": "smallint",
            "int4": "integer",
            "int8": "bigint",
            "float4": "real",
            "float8": "double precision",
            "bool": "boolean",
            "bpchar": "character",
            "varchar": "character varying",
            "timestamptz": "timestamp with time zone",
        }
        base = internal_to_readable.get(udt_name.lstrip("_"), udt_name.lstrip("_"))
        return f"{base}[]"

    if data_type == "USER-DEFINED":
        return udt_name

    if data_type in {"character varying", "varchar", "character", "char"}:
        if char_max_len:
            return f"{data_type}({char_max_len})"
        return data_type

    if data_type in {"numeric", "decimal"}:
        if num_precision is not None and num_scale is not None:
            return f"{data_type}({num_precision},{num_scale})"
        return data_type

    if data_type in {"bit", "bit varying"}:
        if char_max_len:
            return f"{data_type}({char_max_len})"
        return data_type

    return data_type


def _parse_pg_array(raw: str | None) -> list[str] | None:
    """Parse a simple PostgreSQL text array like '{a,\"b\"}' into Python strings."""
    if raw is None:
        return None
    inner = raw.strip("{}")
    if not inner:
        return []
    parsed = next(reader(StringIO(inner), delimiter=",", quotechar='"', escapechar="\\"))
    return list(parsed)


class PostgreSQLConnector(BaseConnector):
    """Concrete PostgreSQL connector backed by psycopg2 and a threaded pool."""

    def __init__(self, config: AtlasConnectionConfig) -> None:
        super().__init__(config)
        self._pool: Any | None = None
        self._server_version = ""
        self._server_version_info: tuple[int, int, int] = (0, 0, 0)

    def connect(self) -> None:
        """Create the connection pool and detect the server version."""
        if self._connected and self._pool is not None:
            return

        psycopg2 = _require_psycopg2()
        pool_max = int(self._config.connect_args.get("pool_max", _DEFAULT_POOL_MAX))

        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=pool_max,
                connect_timeout=self._config.timeout_seconds,
                **self._build_connect_kwargs(),
            )
        except Exception as exc:  # pragma: no cover - exercised via tests with mocks
            raise ConnectionError(
                f"Failed to connect to {self._config.connection_string_safe}: {exc}"
            ) from exc

        self._connected = True
        self._server_version, self._server_version_info = self._detect_server_version()

    def disconnect(self) -> None:
        """Close every pooled connection."""
        if self._pool is not None:
            try:
                self._pool.closeall()
            finally:
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
        """Return the detected PostgreSQL server version."""
        return self._server_version

    @property
    def server_version_info(self) -> tuple[int, int, int]:
        """Return the parsed `(major, minor, patch)` version tuple."""
        return self._server_version_info

    def _build_connect_kwargs(self) -> dict[str, Any]:
        cfg = self._config
        ssl_mode = "prefer" if cfg.ssl_mode == "preferred" else cfg.ssl_mode
        payload: dict[str, Any] = {
            "host": cfg.host,
            "dbname": cfg.database,
            "sslmode": ssl_mode,
        }
        if cfg.port is not None:
            payload["port"] = cfg.port
        if cfg.user:
            payload["user"] = cfg.user
        if cfg.password:
            payload["password"] = cfg.password
        if cfg.ssl_mode in {"verify-ca", "verify-full", "require"}:
            for ssl_key in ("sslcert", "sslkey", "sslrootcert"):
                if ssl_key in cfg.connect_args:
                    payload[ssl_key] = cfg.connect_args[ssl_key]
        payload["application_name"] = cfg.connect_args.get("application_name", "atlas-datamap")
        return payload

    def _build_dsn(self) -> str:
        return " ".join(f"{key}={value}" for key, value in self._build_connect_kwargs().items())

    @contextmanager
    def _cursor(self) -> Generator[Any, None, None]:
        """Borrow a pooled connection, configure it as read-only, and yield a cursor."""
        if self._pool is None:
            raise ConnectionError("PostgreSQL connector is not connected. Call connect() first.")

        connection = self._pool.getconn()
        try:
            connection.set_session(readonly=True, autocommit=False)
            statement_timeout_ms = self._config.timeout_seconds * 1000
            with connection.cursor() as setup_cursor:
                setup_cursor.execute(f"SET statement_timeout = {statement_timeout_ms}")
                setup_cursor.execute("SET lock_timeout = 5000")
            with connection.cursor() as cursor:
                yield cursor
        except QueryError:
            raise
        except Exception as exc:
            raise QueryError(f"PostgreSQL query failed: {exc}") from exc
        finally:
            with suppress(Exception):
                connection.rollback()
            self._pool.putconn(connection)

    def _detect_server_version(self) -> tuple[str, tuple[int, int, int]]:
        with self._cursor() as cursor:
            cursor.execute("SELECT version(), current_setting('server_version_num')")
            row = cursor.fetchone()
        if not row:
            return "unknown", (0, 0, 0)
        version_text = str(row[0])
        server_version_num = int(row[1])
        major = server_version_num // 10000
        minor = (server_version_num % 10000) // 100
        patch = server_version_num % 100
        parts = version_text.split()
        display = f"{parts[0]} {parts[1]}" if len(parts) >= 2 else version_text
        return display, (major, minor, patch)

    def _quote_identifier(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

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
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT LIKE 'pg_%'
              AND schema_name NOT IN ('information_schema')
            ORDER BY schema_name
        """
        rows = self._fetchall(sql, ())
        schemas: list[SchemaInfo] = []
        for (schema_name,) in rows:
            if self._should_include_schema(schema_name):
                schemas.append(SchemaInfo(name=schema_name, engine=self._config.engine.value))
        return schemas

    def get_tables(self, schema: str) -> list[TableInfo]:
        sql_tables = """
            SELECT
                t.table_name,
                t.table_type,
                obj_description(c.oid, 'pg_class') AS comment
            FROM information_schema.tables t
            JOIN pg_class c
              ON c.relname = t.table_name
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
             AND n.nspname = t.table_schema
            WHERE t.table_schema = %s
              AND t.table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY t.table_name
        """
        sql_mviews = """
            SELECT
                m.matviewname,
                obj_description(c.oid, 'pg_class') AS comment
            FROM pg_matviews m
            JOIN pg_class c
              ON c.relname = m.matviewname
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
             AND n.nspname = m.schemaname
            WHERE m.schemaname = %s
            ORDER BY m.matviewname
        """
        tables: list[TableInfo] = []
        for table_name, raw_type, comment in self._fetchall(sql_tables, (schema,)):
            table_type = TableType.VIEW if raw_type == "VIEW" else TableType.TABLE
            tables.append(
                TableInfo(
                    name=str(table_name),
                    schema=schema,
                    table_type=table_type,
                    comment=str(comment) if comment is not None else None,
                )
            )
        for table_name, comment in self._fetchall(sql_mviews, (schema,)):
            tables.append(
                TableInfo(
                    name=str(table_name),
                    schema=schema,
                    table_type=TableType.MATERIALIZED_VIEW,
                    comment=str(comment) if comment is not None else None,
                )
            )
        return sorted(tables, key=lambda table: table.name)

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        sql = """
            SELECT COALESCE(
                (
                    SELECT n_live_tup
                    FROM pg_stat_user_tables
                    WHERE schemaname = %s AND relname = %s
                ),
                (
                    SELECT reltuples::BIGINT
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                      AND reltuples > 0
                ),
                0
            )::BIGINT
        """
        row = self._fetchone(sql, (schema, table, schema, table))
        return int(row[0]) if row else 0

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        sql = """
            SELECT COALESCE(pg_relation_size(c.oid), 0)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """
        row = self._fetchone(sql, (schema, table))
        return int(row[0]) if row else 0

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        sql = """
            SELECT
                col.column_name,
                col.data_type,
                col.udt_name,
                col.character_maximum_length,
                col.numeric_precision,
                col.numeric_scale,
                col.is_nullable,
                col.column_default,
                col.ordinal_position,
                CASE WHEN pk.attname IS NOT NULL THEN TRUE ELSE FALSE END AS is_pk,
                col_desc.description AS column_comment
            FROM information_schema.columns col
            LEFT JOIN (
                SELECT a.attname
                FROM pg_constraint con
                JOIN pg_class cls ON cls.oid = con.conrelid
                JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                JOIN pg_attribute a
                  ON a.attrelid = cls.oid
                 AND a.attnum = ANY(con.conkey)
                WHERE con.contype = 'p'
                  AND ns.nspname = %s
                  AND cls.relname = %s
            ) pk ON pk.attname = col.column_name
            LEFT JOIN (
                SELECT a.attname, d.description
                FROM pg_attribute a
                JOIN pg_class cls ON cls.oid = a.attrelid
                JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                JOIN pg_description d
                  ON d.objoid = cls.oid
                 AND d.objsubid = a.attnum
                WHERE ns.nspname = %s
                  AND cls.relname = %s
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            ) col_desc ON col_desc.attname = col.column_name
            WHERE col.table_schema = %s
              AND col.table_name = %s
            ORDER BY col.ordinal_position
        """
        rows = self._fetchall(sql, (schema, table, schema, table, schema, table))
        columns: list[ColumnInfo] = []
        for row in rows:
            (
                column_name,
                data_type,
                udt_name,
                char_max_len,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default,
                ordinal_position,
                is_primary_key,
                comment,
            ) = row
            native_type = _compose_native_type(
                str(data_type),
                str(udt_name),
                int(char_max_len) if char_max_len is not None else None,
                int(numeric_precision) if numeric_precision is not None else None,
                int(numeric_scale) if numeric_scale is not None else None,
            )
            default_value = str(column_default) if column_default is not None else None
            lower_default = default_value.lower() if default_value is not None else ""
            is_auto_increment = default_value is not None and (
                "nextval(" in default_value or "generated" in lower_default
            )
            columns.append(
                ColumnInfo(
                    name=str(column_name),
                    native_type=native_type,
                    canonical_type=normalize_type(native_type, "postgresql"),
                    ordinal=int(ordinal_position or 0),
                    is_nullable=str(is_nullable) == "YES",
                    is_primary_key=bool(is_primary_key),
                    is_auto_increment=is_auto_increment,
                    default_value=default_value,
                    comment=str(comment) if comment is not None else None,
                )
            )
        return columns

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        sql = """
            SELECT
                con.conname,
                src_ns.nspname,
                src_cls.relname,
                ARRAY(
                    SELECT a.attname
                    FROM pg_attribute a
                    WHERE a.attrelid = src_cls.oid
                      AND a.attnum = ANY(con.conkey)
                    ORDER BY array_position(con.conkey, a.attnum)
                ) AS source_columns,
                tgt_ns.nspname,
                tgt_cls.relname,
                ARRAY(
                    SELECT a.attname
                    FROM pg_attribute a
                    WHERE a.attrelid = tgt_cls.oid
                      AND a.attnum = ANY(con.confkey)
                    ORDER BY array_position(con.confkey, a.attnum)
                ) AS target_columns,
                CASE con.confdeltype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                END AS on_delete,
                CASE con.confupdtype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                END AS on_update
            FROM pg_constraint con
            JOIN pg_class src_cls ON src_cls.oid = con.conrelid
            JOIN pg_namespace src_ns ON src_ns.oid = src_cls.relnamespace
            JOIN pg_class tgt_cls ON tgt_cls.oid = con.confrelid
            JOIN pg_namespace tgt_ns ON tgt_ns.oid = tgt_cls.relnamespace
            WHERE con.contype = 'f'
              AND src_ns.nspname = %s
              AND src_cls.relname = %s
            ORDER BY con.conname
        """
        rows = self._fetchall(sql, (schema, table))
        return [
            ForeignKeyInfo(
                name=str(row[0]),
                source_schema=str(row[1]),
                source_table=str(row[2]),
                source_columns=list(row[3]),
                target_schema=str(row[4]),
                target_table=str(row[5]),
                target_columns=list(row[6]),
                on_delete=str(row[7] or "NO ACTION"),
                on_update=str(row[8] or "NO ACTION"),
                is_inferred=False,
            )
            for row in rows
        ]

    def infer_implicit_fks(
        self,
        schema: str,
        table: str,
        columns: list[ColumnInfo],
        all_table_names: set[str],
        declared_fks: list[ForeignKeyInfo],
    ) -> list[ForeignKeyInfo]:
        """Infer foreign keys from `<name>_id` conventions when no FK is declared."""
        declared_columns = {
            column_name for foreign_key in declared_fks for column_name in foreign_key.source_columns
        }
        inferred: list[ForeignKeyInfo] = []
        for column in columns:
            if column.name in declared_columns:
                continue
            if not column.name.endswith("_id"):
                continue
            if column.canonical_type not in {AtlasType.INTEGER, AtlasType.UNKNOWN}:
                continue
            base = column.name[:-3]
            candidates = [base, f"{base}s", f"{base}es"]
            if base.endswith("ao"):
                candidates.append(f"{base[:-2]}oes")
            target_table = next((candidate for candidate in candidates if candidate in all_table_names), None)
            if target_table is None:
                continue
            inferred.append(
                ForeignKeyInfo(
                    name="",
                    source_schema=schema,
                    source_table=table,
                    source_columns=[column.name],
                    target_schema=schema,
                    target_table=target_table,
                    target_columns=["id"],
                    on_delete="NO ACTION",
                    on_update="NO ACTION",
                    is_inferred=True,
                )
            )
        return inferred

    def get_indexes(self, schema: str, table: str) -> list[IndexInfo]:
        sql = """
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                am.amname AS index_type,
                (ix.indpred IS NOT NULL) AS is_partial,
                ARRAY(
                    SELECT a.attname
                    FROM pg_attribute a
                    WHERE a.attrelid = t.oid
                      AND a.attnum = ANY(ix.indkey)
                      AND a.attnum > 0
                    ORDER BY array_position(ix.indkey::int[], a.attnum::int)
                ) AS columns
            FROM pg_index ix
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            WHERE n.nspname = %s
              AND t.relname = %s
            ORDER BY i.relname
        """
        rows = self._fetchall(sql, (schema, table))
        return [
            IndexInfo(
                name=str(index_name),
                table=table,
                schema=schema,
                columns=list(columns),
                is_unique=bool(is_unique),
                is_primary=bool(is_primary),
                is_partial=bool(is_partial),
                index_type=str(index_type or "btree"),
            )
            for index_name, is_unique, is_primary, index_type, is_partial, columns in rows
        ]

    @staticmethod
    def detect_redundant_indexes(indexes: list[IndexInfo]) -> list[str]:
        """Detect indexes whose column list is a strict prefix of another non-partial index."""
        redundant: list[str] = []
        non_partial = [index for index in indexes if not index.is_partial]
        for left in non_partial:
            for right in non_partial:
                if left.name == right.name:
                    continue
                if len(left.columns) >= len(right.columns):
                    continue
                if right.columns[: len(left.columns)] == left.columns and left.name not in redundant:
                    redundant.append(left.name)
                    break
        return redundant

    def _fetch_pg_stats_row(self, schema: str, table: str, column: str) -> Any | None:
        sql = """
            SELECT
                null_frac,
                n_distinct,
                histogram_bounds::text
            FROM pg_stats
            WHERE schemaname = %s
              AND tablename = %s
              AND attname = %s
        """
        return self._fetchone(sql, (schema, table, column))

    def get_column_stats(self, schema: str, table: str, column: str) -> ColumnStats:
        row_count = self.get_row_count_estimate(schema, table)
        row = self._fetch_pg_stats_row(schema, table, column)
        if row is None:
            return ColumnStats(row_count=row_count)

        null_frac, n_distinct, histogram_bounds = row
        null_count = 0
        if null_frac is not None and row_count > 0:
            null_count = max(0, int(float(null_frac) * row_count))

        distinct_count = 0
        if n_distinct is not None:
            if float(n_distinct) > 0:
                distinct_count = int(float(n_distinct))
            elif float(n_distinct) <= -0.999:
                distinct_count = row_count
            else:
                distinct_count = int(abs(float(n_distinct)) * row_count)

        bounds = _parse_pg_array(str(histogram_bounds) if histogram_bounds is not None else None) or []
        min_value = bounds[0] if bounds else ""
        max_value = bounds[-1] if bounds else ""

        return ColumnStats(
            row_count=row_count,
            null_count=null_count,
            distinct_count=distinct_count,
            min_value=min_value,
            max_value=max_value,
        )

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        row = self._fetch_pg_stats_row(schema, table, column)
        row_count = self.get_row_count_estimate(schema, table)
        if row is None or row[0] is None or row_count <= 0:
            return 0
        return max(0, int(float(row[0]) * row_count))

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        row = self._fetch_pg_stats_row(schema, table, column)
        row_count = self.get_row_count_estimate(schema, table)
        if row is None or row[1] is None:
            return 0
        n_distinct = float(row[1])
        if n_distinct > 0:
            return int(n_distinct)
        if n_distinct <= -0.999:
            return row_count
        return int(abs(n_distinct) * row_count)

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

        row_estimate = self.get_row_count_estimate(schema, table)
        selected_columns = (
            "*"
            if not columns
            else ", ".join(self._quote_identifier(column_name) for column_name in columns)
        )
        qualified_name = self._qualified_name(schema, table)

        if row_estimate >= TABLESAMPLE_THRESHOLD:
            percentage = min(100.0, 100.0 * effective_limit / max(row_estimate, 1))
            sql = (
                f"SELECT {selected_columns} FROM {qualified_name} "
                "TABLESAMPLE SYSTEM(%s) LIMIT %s"
            )
            params: tuple[Any, ...] = (percentage, effective_limit)
        else:
            sql = f"SELECT {selected_columns} FROM {qualified_name} LIMIT %s"
            params = (effective_limit,)

        with self._cursor() as cursor:
            cursor.execute(sql, params)
            if cursor.description is None:
                return []
            column_names = [description[0] for description in cursor.description]
            rows = cursor.fetchall()

        if not rows and row_estimate >= TABLESAMPLE_THRESHOLD:
            with self._cursor() as cursor:
                cursor.execute(f"SELECT {selected_columns} FROM {qualified_name} LIMIT %s", (effective_limit,))
                if cursor.description is None:
                    return []
                column_names = [description[0] for description in cursor.description]
                rows = cursor.fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            record = dict(zip(column_names, row, strict=False))
            result.append(self._mask_row(record, effective_mode))
        return result

    def introspect_schema(self, schema_name: str) -> SchemaInfo:
        """Override the base orchestration to append implicit foreign keys after column discovery."""
        self._logger.info("Introspecting PostgreSQL schema %s", schema_name)
        tables = self.get_tables(schema_name)
        all_table_names = {table.name for table in tables if table.table_type is TableType.TABLE}
        total_size_bytes = 0

        for table in tables:
            table.columns = self.get_columns(schema_name, table.name)
            declared_foreign_keys = self.get_foreign_keys(schema_name, table.name)
            inferred_foreign_keys = self.infer_implicit_fks(
                schema_name,
                table.name,
                table.columns,
                all_table_names,
                declared_foreign_keys,
            )
            table.foreign_keys = declared_foreign_keys + inferred_foreign_keys
            table.indexes = self.get_indexes(schema_name, table.name)
            table.column_count = len(table.columns)
            table.row_count_estimate = self.get_row_count_estimate(schema_name, table.name)
            table.size_bytes = self.get_table_size_bytes(schema_name, table.name)
            total_size_bytes += table.size_bytes

            indexed_columns = {column for index in table.indexes for column in index.columns}
            fk_source_columns = {
                column_name
                for foreign_key in table.foreign_keys
                for column_name in foreign_key.source_columns
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
        """Use the PostgreSQL-specific schema introspection while preserving the Phase 0 result contract."""
        schemas = [self.introspect_schema(schema.name) for schema in self.get_schemas()]
        fk_in_degree_map: dict[str, list[str]] = {}
        for schema in schemas:
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
            schemas=schemas,
            fk_in_degree_map=fk_in_degree_map,
            introspected_at=datetime.datetime.now(datetime.UTC).isoformat(),
        )

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        version_suffix = f" [{self._server_version}]" if self._server_version else ""
        return (
            f"PostgreSQLConnector({self._config.connection_string_safe}"
            f"{version_suffix}, {status})"
        )
