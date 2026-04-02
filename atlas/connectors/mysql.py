"""MySQL and MariaDB connector for Atlas Datamap."""

from __future__ import annotations

import logging
import random
import re
from collections.abc import Generator
from contextlib import contextmanager, suppress
from typing import Any

from atlas.config import AtlasConnectionConfig, PrivacyMode
from atlas.connectors.base import BaseConnector, ConnectionError, QueryError
from atlas.connectors.type_mapping import normalize_type
from atlas.types import ColumnInfo, ForeignKeyInfo, IndexInfo, SchemaInfo, TableInfo, TableType

logger = logging.getLogger(__name__)

RAND_THRESHOLD = 5_000
_DEFAULT_POOL_SIZE = 5
_SYSTEM_SCHEMAS = frozenset({"information_schema", "mysql", "performance_schema", "sys"})


def _require_mysql_connector() -> Any:
    """Import mysql.connector lazily so Atlas can import without the optional driver."""
    try:
        import mysql.connector
        import mysql.connector.pooling

        return mysql.connector
    except ImportError as exc:  # pragma: no cover - exercised when the driver is absent
        raise ImportError(
            "mysql-connector-python is not installed. "
            "Run 'pip install \"atlas-datamap[mysql]\"' or install 'mysql-connector-python'."
        ) from exc


class MySQLConnector(BaseConnector):
    """Concrete MySQL and MariaDB connector backed by mysql.connector pooling."""

    def __init__(self, config: AtlasConnectionConfig) -> None:
        super().__init__(config)
        self._pool: Any | None = None
        self._server_version = ""
        self._server_version_info: tuple[int, int, int] = (0, 0, 0)
        self._is_mariadb = False

    def connect(self) -> None:
        """Create the connection pool and detect server version and variant."""
        if self._connected and self._pool is not None:
            return

        mysql_connector = _require_mysql_connector()
        pool_size = int(self._config.connect_args.get("pool_size", _DEFAULT_POOL_SIZE))
        payload = self._build_connect_kwargs()
        payload["pool_name"] = f"atlas_mysql_{id(self)}"
        payload["pool_size"] = pool_size

        try:
            self._pool = mysql_connector.pooling.MySQLConnectionPool(**payload)
        except Exception as exc:  # pragma: no cover - exercised via unit tests with mocks
            raise ConnectionError(
                f"Failed to connect to {self._config.connection_string_safe}: {exc}"
            ) from exc

        self._connected = True
        self._server_version, self._server_version_info, self._is_mariadb = (
            self._detect_server_version()
        )

    def disconnect(self) -> None:
        """Release the pool reference and mark the connector as disconnected."""
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
        """Return the detected server version string."""
        return self._server_version

    @property
    def server_version_info(self) -> tuple[int, int, int]:
        """Return the parsed server version tuple."""
        return self._server_version_info

    @property
    def is_mariadb(self) -> bool:
        """Return whether the detected server is MariaDB instead of upstream MySQL."""
        return self._is_mariadb

    def _build_connect_kwargs(self) -> dict[str, Any]:
        cfg = self._config
        payload: dict[str, Any] = {
            "host": cfg.host,
            "database": cfg.database,
            "charset": "utf8mb4",
            "use_unicode": True,
            "autocommit": False,
            "connection_timeout": cfg.timeout_seconds,
        }
        if cfg.port is not None:
            payload["port"] = cfg.port
        if cfg.user:
            payload["user"] = cfg.user
        if cfg.password:
            payload["password"] = cfg.password

        ssl_mode = cfg.ssl_mode
        if ssl_mode == "disable":
            payload["ssl_disabled"] = True
        else:
            payload["ssl_disabled"] = False
            if ssl_mode == "verify-full":
                payload["ssl_verify_cert"] = True
                payload["ssl_verify_identity"] = True
            elif ssl_mode == "verify-ca":
                payload["ssl_verify_cert"] = True
            for key in ("ssl_ca", "ssl_cert", "ssl_key"):
                if key in cfg.connect_args:
                    payload[key] = cfg.connect_args[key]
        return payload

    def _build_dsn(self) -> str:
        return " ".join(f"{key}={value}" for key, value in self._build_connect_kwargs().items())

    @contextmanager
    def _cursor(self) -> Generator[Any, None, None]:
        """Borrow a pooled connection, start a read-only transaction, and yield a cursor."""
        if self._pool is None:
            raise ConnectionError("MySQL connector is not connected. Call connect() first.")

        connection = self._pool.get_connection()
        try:
            statement_timeout_ms = self._config.timeout_seconds * 1000
            with connection.cursor() as setup_cursor:
                try:
                    setup_cursor.execute("START TRANSACTION READ ONLY")
                except Exception:
                    setup_cursor.execute("START TRANSACTION")
                if self._is_mariadb:
                    setup_cursor.execute(
                        "SET SESSION max_statement_time = %s", (max(1, statement_timeout_ms / 1000),)
                    )
                else:
                    try:
                        setup_cursor.execute(
                            "SET SESSION max_execution_time = %s", (statement_timeout_ms,)
                        )
                    except Exception:
                        setup_cursor.execute(
                            "SET SESSION max_statement_time = %s",
                            (max(1, statement_timeout_ms / 1000),),
                        )
            with connection.cursor() as cursor:
                yield cursor
        except QueryError:
            raise
        except Exception as exc:
            raise QueryError(f"MySQL query failed: {exc}") from exc
        finally:
            with suppress(Exception):
                connection.rollback()
            with suppress(Exception):
                connection.close()

    def _detect_server_version(self) -> tuple[str, tuple[int, int, int], bool]:
        with self._cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            row = cursor.fetchone()
        version_text = str(row[0]) if row else "0.0.0"
        is_mariadb = "mariadb" in version_text.lower()
        match = re.search(r"(\d+)\.(\d+)\.(\d+)", version_text)
        if match is None:
            return version_text, (0, 0, 0), is_mariadb
        return (
            version_text,
            (int(match.group(1)), int(match.group(2)), int(match.group(3))),
            is_mariadb,
        )

    def _quote_identifier(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

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
            WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY schema_name
        """
        rows = self._fetchall(sql, ())
        schemas: list[SchemaInfo] = []
        for (schema_name,) in rows:
            normalized_schema = str(schema_name)
            if normalized_schema in _SYSTEM_SCHEMAS:
                continue
            if self._should_include_schema(normalized_schema):
                schemas.append(
                    SchemaInfo(
                        name=normalized_schema,
                        engine="mariadb" if self.is_mariadb else self._config.engine.value,
                    )
                )
        return schemas

    def _get_mariadb_routines(self, schema: str) -> list[dict[str, str]]:
        sql = """
            SELECT ROUTINE_NAME, ROUTINE_TYPE, COALESCE(ROUTINE_COMMENT, '')
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = %s
            ORDER BY ROUTINE_NAME
        """
        routines: list[dict[str, str]] = []
        for routine_name, routine_type, routine_comment in self._fetchall(sql, (schema,)):
            routines.append(
                {
                    "name": str(routine_name),
                    "type": str(routine_type),
                    "comment": str(routine_comment or ""),
                }
            )
        return routines

    def _get_mariadb_sequences(self, schema: str) -> list[str]:
        try:
            return [
                str(sequence_name)
                for (sequence_name,) in self._fetchall(
                    """
                    SELECT SEQUENCE_NAME
                    FROM information_schema.SEQUENCES
                    WHERE SEQUENCE_SCHEMA = %s
                    ORDER BY SEQUENCE_NAME
                    """,
                    (schema,),
                )
            ]
        except Exception:
            try:
                return [
                    str(sequence_name)
                    for (sequence_name,) in self._fetchall(
                        """
                        SELECT TABLE_NAME
                        FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = %s
                          AND TABLE_TYPE = 'SEQUENCE'
                        ORDER BY TABLE_NAME
                        """,
                        (schema,),
                    )
                ]
            except Exception:
                return []

    def _enrich_schema_for_mariadb(self, schema: SchemaInfo) -> SchemaInfo:
        schema.engine = "mariadb"
        schema.extra_metadata["mariadb_routines"] = self._get_mariadb_routines(schema.name)
        schema.extra_metadata["mariadb_sequences"] = self._get_mariadb_sequences(schema.name)
        return schema

    def get_tables(self, schema: str) -> list[TableInfo]:
        sql = """
            SELECT
                table_name,
                table_type,
                NULLIF(table_comment, '') AS table_comment,
                COALESCE(table_rows, 0) AS row_estimate,
                COALESCE(data_length + index_length, 0) AS size_bytes
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_name
        """
        tables: list[TableInfo] = []
        for table_name, raw_type, comment, row_estimate, size_bytes in self._fetchall(sql, (schema,)):
            table_type = TableType.VIEW if str(raw_type) == "VIEW" else TableType.TABLE
            tables.append(
                TableInfo(
                    name=str(table_name),
                    schema=schema,
                    table_type=table_type,
                    comment=str(comment) if comment is not None else None,
                    row_count_estimate=int(row_estimate or 0),
                    size_bytes=int(size_bytes or 0),
                )
            )
        return tables

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        sql = """
            SELECT COALESCE(table_rows, 0)
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        """
        row = self._fetchone(sql, (schema, table))
        return int(row[0]) if row else 0

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        sql = """
            SELECT COALESCE(data_length + index_length, 0)
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        """
        row = self._fetchone(sql, (schema, table))
        return int(row[0]) if row else 0

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        sql = """
            SELECT
                c.column_name,
                c.column_type,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.ordinal_position,
                c.column_key,
                NULLIF(c.column_comment, '') AS column_comment,
                c.extra,
                jc.check_clause
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT
                    tc.table_schema,
                    tc.table_name,
                    tc.constraint_name AS column_name,
                    cc.check_clause
                FROM information_schema.table_constraints tc
                JOIN information_schema.check_constraints cc
                  ON cc.constraint_schema = tc.constraint_schema
                 AND cc.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'CHECK'
            ) jc
              ON jc.table_schema = c.table_schema
             AND jc.table_name = c.table_name
             AND jc.column_name = c.column_name
            WHERE c.table_schema = %s
              AND c.table_name = %s
            ORDER BY c.ordinal_position
        """
        columns: list[ColumnInfo] = []
        for row in self._fetchall(sql, (schema, table)):
            (
                column_name,
                column_type,
                data_type,
                is_nullable,
                column_default,
                ordinal_position,
                column_key,
                comment,
                extra,
                check_clause,
            ) = row
            native_type = str(column_type or data_type or "unknown")
            if self.is_mariadb:
                normalized_check = str(check_clause or "").lower()
                if (
                    str(data_type or "").lower() == "longtext"
                    and f"json_valid(`{column_name}`)" in normalized_check
                ):
                    native_type = "json"
            columns.append(
                ColumnInfo(
                    name=str(column_name),
                    native_type=native_type,
                    canonical_type=normalize_type(
                        native_type,
                        "mariadb" if self.is_mariadb else "mysql",
                    ),
                    ordinal=int(ordinal_position or 0),
                    is_nullable=str(is_nullable) == "YES",
                    is_primary_key=str(column_key) == "PRI",
                    is_unique=str(column_key) == "UNI",
                    is_auto_increment="auto_increment" in str(extra or "").lower(),
                    default_value=str(column_default) if column_default is not None else None,
                    comment=str(comment) if comment is not None else None,
                )
            )
        return columns

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        sql = """
            SELECT
                kcu.constraint_name,
                kcu.column_name,
                kcu.referenced_table_schema,
                kcu.referenced_table_name,
                kcu.referenced_column_name,
                rc.delete_rule,
                rc.update_rule
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.referential_constraints rc
              ON rc.constraint_schema = kcu.constraint_schema
             AND rc.constraint_name = kcu.constraint_name
             AND rc.table_name = kcu.table_name
            WHERE kcu.table_schema = %s
              AND kcu.table_name = %s
              AND kcu.referenced_table_name IS NOT NULL
            ORDER BY kcu.constraint_name, kcu.ordinal_position
        """
        grouped: dict[str, ForeignKeyInfo] = {}
        for row in self._fetchall(sql, (schema, table)):
            (
                constraint_name,
                column_name,
                referenced_schema,
                referenced_table,
                referenced_column,
                delete_rule,
                update_rule,
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
                    on_delete=str(delete_rule or "NO ACTION"),
                    on_update=str(update_rule or "NO ACTION"),
                    is_inferred=False,
                )
            grouped[name].source_columns.append(str(column_name))
            grouped[name].target_columns.append(str(referenced_column))
        return list(grouped.values())

    def get_indexes(self, schema: str, table: str) -> list[IndexInfo]:
        sql = """
            SELECT
                index_name,
                column_name,
                non_unique,
                index_type,
                seq_in_index,
                sub_part
            FROM information_schema.statistics
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY index_name, seq_in_index
        """
        grouped: dict[str, IndexInfo] = {}
        for row in self._fetchall(sql, (schema, table)):
            index_name, column_name, non_unique, index_type, _seq_in_index, sub_part = row
            name = str(index_name)
            if name not in grouped:
                grouped[name] = IndexInfo(
                    name=name,
                    table=table,
                    schema=schema,
                    columns=[],
                    is_unique=not bool(non_unique),
                    is_primary=name == "PRIMARY",
                    is_partial=sub_part is not None,
                    index_type=str(index_type or "BTREE").lower(),
                )
            grouped[name].columns.append(str(column_name))
            if sub_part is not None:
                grouped[name].is_partial = True
        return list(grouped.values())

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        """Return zero when catalog statistics do not expose a null-count estimate cheaply."""
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        sql = """
            SELECT MAX(COALESCE(cardinality, 0))
            FROM information_schema.statistics
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
              AND seq_in_index = 1
        """
        row = self._fetchone(sql, (schema, table, column))
        return int(row[0]) if row and row[0] is not None else 0

    def introspect_schema(self, schema_name: str) -> SchemaInfo:
        schema = super().introspect_schema(schema_name)
        if self.is_mariadb:
            schema = self._enrich_schema_for_mariadb(schema)
        return schema

    def introspect_all(self) -> Any:
        result = super().introspect_all()
        if self.is_mariadb:
            result.engine = "mariadb"
            for schema in result.schemas:
                schema.engine = "mariadb"
        return result

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

        if row_estimate <= RAND_THRESHOLD:
            sql = f"SELECT {selected_columns} FROM {qualified_name} ORDER BY RAND() LIMIT %s"
            params: tuple[Any, ...] = (effective_limit,)
        else:
            max_offset = max(0, row_estimate - effective_limit)
            offset = random.randint(0, max_offset) if max_offset else 0
            sql = f"SELECT {selected_columns} FROM {qualified_name} LIMIT %s OFFSET %s"
            params = (effective_limit, offset)

        with self._cursor() as cursor:
            cursor.execute(sql, params)
            if cursor.description is None:
                return []
            column_names = [description[0] for description in cursor.description]
            rows = cursor.fetchall()

        if not rows and row_estimate > RAND_THRESHOLD:
            with self._cursor() as cursor:
                cursor.execute(f"SELECT {selected_columns} FROM {qualified_name} LIMIT %s", (effective_limit,))
                if cursor.description is None:
                    return []
                column_names = [description[0] for description in cursor.description]
                rows = cursor.fetchall()

        return [
            self._mask_row(dict(zip(column_names, row, strict=False)), effective_mode) for row in rows
        ]

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        variant = "MariaDB" if self._is_mariadb else "MySQL"
        version_suffix = f" [{variant} {self._server_version}]" if self._server_version else ""
        return f"MySQLConnector({self._config.connection_string_safe}{version_suffix}, {status})"
