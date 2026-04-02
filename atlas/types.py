"""Canonical metadata types shared by every Atlas module."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class AtlasType(StrEnum):
    """Canonical column type normalization for every connector."""

    INTEGER = "integer"
    SMALLINT = "smallint"
    BIGINT = "bigint"
    TINYINT = "tinyint"
    FLOAT = "float"
    DOUBLE = "double"
    TEXT = "text"
    CHAR = "char"
    CLOB = "clob"
    DECIMAL = "decimal"
    MONEY = "money"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    DATE = "date"
    TIME = "time"
    INTERVAL = "interval"
    BINARY = "binary"
    JSON = "json"
    XML = "xml"
    ARRAY = "array"
    ENUM = "enum"
    UUID = "uuid"
    SPATIAL = "spatial"
    UNKNOWN = "unknown"

    @classmethod
    def from_native(cls, native_type: str, engine: str = "") -> AtlasType:
        if engine:
            from atlas.connectors.type_mapping import normalize_type

            return normalize_type(native_type, engine)

        normalized = native_type.strip().lower()
        if not normalized:
            return cls.UNKNOWN
        if normalized.endswith("[]") or normalized.startswith("_"):
            return cls.ARRAY
        if normalized in {"tinyint(1)", "bit(1)", "bool", "boolean"}:
            return cls.BOOLEAN

        base = re.sub(r"\s*\([^)]*\)", "", normalized).strip()
        base = re.sub(r"\s+unsigned$", "", base).strip()

        if base in {"smallint", "int2", "smallserial"}:
            return cls.SMALLINT
        if base in {"bigint", "int8", "bigserial"}:
            return cls.BIGINT
        if base in {"tinyint"}:
            return cls.TINYINT
        if base in {"int", "int4", "integer", "serial", "mediumint", "oid", "xid", "cid"}:
            return cls.INTEGER
        if base in {"float", "real", "float4"}:
            return cls.FLOAT
        if base in {"double", "double precision", "float8"}:
            return cls.DOUBLE
        if base in {"decimal", "numeric", "dec", "fixed"}:
            return cls.DECIMAL
        if base in {"money", "smallmoney"}:
            return cls.MONEY
        if base in {"bit"}:
            return cls.BOOLEAN
        if base in {"uuid", "uniqueidentifier"}:
            return cls.UUID
        if base in {"json", "jsonb", "hstore"}:
            return cls.JSON
        if base in {"xml"}:
            return cls.XML
        if base in {"timestamp with time zone", "timestamptz", "datetimeoffset"}:
            return cls.TIMESTAMP
        if base in {"timestamp", "timestamp without time zone", "datetime", "datetime2", "smalldatetime"}:
            return cls.DATETIME
        if base == "date" or base == "year":
            return cls.DATE
        if base in {"time", "timetz", "time with time zone", "time without time zone"}:
            return cls.TIME
        if base == "interval":
            return cls.INTERVAL
        if base in {
            "bytea",
            "blob",
            "mediumblob",
            "longblob",
            "tinyblob",
            "varbinary",
            "binary",
            "image",
            "raw",
            "rowversion",
            "bit varying",
            "varbit",
        }:
            return cls.BINARY
        if base in {"enum", "set"}:
            return cls.ENUM
        if base == "user-defined":
            return cls.UNKNOWN
        if base in {
            "geometry",
            "geography",
            "point",
            "line",
            "lseg",
            "box",
            "path",
            "polygon",
            "circle",
            "linestring",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometrycollection",
        }:
            return cls.SPATIAL
        if base in {"text", "ntext", "tinytext", "mediumtext", "longtext", "clob"}:
            return cls.CLOB
        if base in {"character", "char", "bpchar", "nchar"}:
            return cls.CHAR
        if base in {
            "character varying",
            "varchar",
            "nvarchar",
            "citext",
            "name",
            "inet",
            "cidr",
            "macaddr",
            "macaddr8",
            "tsvector",
            "tsquery",
            "sysname",
            "ltree",
            "hierarchyid",
            "string",
        } or "char" in base:
            return cls.TEXT
        return cls.UNKNOWN


class TableType(StrEnum):
    """Structural table category."""

    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    FOREIGN_TABLE = "foreign_table"
    SYNONYM = "synonym"

    @classmethod
    def from_string(cls, value: str) -> TableType:
        mapping = {
            "base table": cls.TABLE,
            "table": cls.TABLE,
            "view": cls.VIEW,
            "materialized view": cls.MATERIALIZED_VIEW,
            "foreign": cls.FOREIGN_TABLE,
            "foreign table": cls.FOREIGN_TABLE,
            "synonym": cls.SYNONYM,
        }
        return mapping.get(value.lower(), cls.TABLE)


@dataclass
class ColumnStats:
    """Column-level aggregate statistics."""

    row_count: int = 0
    null_count: int = 0
    distinct_count: int = 0
    min_value: str = ""
    max_value: str = ""
    avg_length: float = 0.0

    @property
    def null_rate(self) -> float:
        if self.row_count == 0:
            return 0.0
        return min(1.0, self.null_count / self.row_count)

    @property
    def fill_rate(self) -> float:
        return 1.0 - self.null_rate

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ColumnStats:
        return cls(
            **{name: value for name, value in data.items() if name in cls.__dataclass_fields__}
        )


@dataclass
class ColumnInfo:
    """Column metadata."""

    name: str
    native_type: str
    canonical_type: AtlasType | None = None
    ordinal: int = 0
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_unique: bool = False
    is_indexed: bool = False
    default_value: str | None = None
    is_auto_increment: bool = False
    comment: str | None = None
    stats: ColumnStats = field(default_factory=ColumnStats)
    semantic_short: str | None = None
    semantic_detailed: str | None = None
    semantic_role: str | None = None
    semantic_confidence: float = 0.0

    def __post_init__(self) -> None:
        if self.canonical_type is None:
            self.canonical_type = AtlasType.from_native(self.native_type)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["canonical_type"] = (self.canonical_type or AtlasType.UNKNOWN).value
        payload["stats"] = self.stats.to_dict()
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ColumnInfo:
        canonical_raw = data.get("canonical_type")
        stats_raw = data.get("stats", {})
        payload = {name: value for name, value in data.items() if name in cls.__dataclass_fields__}
        payload["canonical_type"] = AtlasType(canonical_raw) if canonical_raw is not None else None
        payload["stats"] = ColumnStats.from_dict(stats_raw)
        return cls(**payload)

    @property
    def is_sensitive_name(self) -> bool:
        from atlas.config import _SENSITIVE_COLUMN_PATTERNS

        lowered = self.name.lower()
        return any(pattern in lowered for pattern in _SENSITIVE_COLUMN_PATTERNS)


@dataclass
class ForeignKeyInfo:
    """Foreign key metadata."""

    name: str
    source_schema: str
    source_table: str
    source_columns: list[str]
    target_schema: str
    target_table: str
    target_columns: list[str]
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"
    is_inferred: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ForeignKeyInfo:
        return cls(
            **{name: value for name, value in data.items() if name in cls.__dataclass_fields__}
        )

    @property
    def source_ref(self) -> str:
        return f"{self.source_schema}.{self.source_table}({', '.join(self.source_columns)})"

    @property
    def target_ref(self) -> str:
        return f"{self.target_schema}.{self.target_table}({', '.join(self.target_columns)})"


@dataclass
class IndexInfo:
    """Index metadata."""

    name: str
    table: str
    schema: str
    columns: list[str]
    is_unique: bool = False
    is_primary: bool = False
    is_partial: bool = False
    index_type: str = "btree"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> IndexInfo:
        return cls(
            **{name: value for name, value in data.items() if name in cls.__dataclass_fields__}
        )


@dataclass
class TableInfo:
    """Table, view, or materialized view metadata."""

    name: str
    schema: str
    table_type: TableType = TableType.TABLE
    row_count_estimate: int = 0
    size_bytes: int = 0
    column_count: int = 0
    comment: str | None = None
    columns: list[ColumnInfo] = field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
    semantic_short: str | None = None
    semantic_detailed: str | None = None
    semantic_domain: str | None = None
    semantic_role: str | None = None
    semantic_confidence: float = 0.0
    heuristic_type: str | None = None
    heuristic_confidence: float = 0.0
    relevance_score: float = 0.0
    _fk_in_degree: int = field(default=-1, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self.refresh_derived_fields()

    def refresh_derived_fields(self) -> None:
        if self.column_count == 0 and self.columns:
            self.column_count = len(self.columns)

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def primary_key_columns(self) -> list[ColumnInfo]:
        return [column for column in self.columns if column.is_primary_key]

    @property
    def foreign_key_columns(self) -> list[ColumnInfo]:
        return [column for column in self.columns if column.is_foreign_key]

    @property
    def fk_in_degree(self) -> int:
        return getattr(self, "_fk_in_degree", -1)

    @property
    def size_bytes_human(self) -> str:
        if self.size_bytes == 0:
            return "unknown"
        thresholds = [("GB", 1_073_741_824), ("MB", 1_048_576), ("KB", 1024)]
        for unit, threshold in thresholds:
            if self.size_bytes >= threshold:
                return f"{self.size_bytes / threshold:.1f} {unit}"
        return f"{self.size_bytes} B"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("_fk_in_degree", None)
        payload["table_type"] = self.table_type.value
        payload["columns"] = [column.to_dict() for column in self.columns]
        payload["foreign_keys"] = [foreign_key.to_dict() for foreign_key in self.foreign_keys]
        payload["indexes"] = [index.to_dict() for index in self.indexes]
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TableInfo:
        payload = {name: value for name, value in data.items() if name in cls.__dataclass_fields__}
        payload["table_type"] = TableType(data.get("table_type", TableType.TABLE.value))
        payload["columns"] = [ColumnInfo.from_dict(item) for item in data.get("columns", [])]
        payload["foreign_keys"] = [
            ForeignKeyInfo.from_dict(item) for item in data.get("foreign_keys", [])
        ]
        payload["indexes"] = [IndexInfo.from_dict(item) for item in data.get("indexes", [])]
        return cls(**payload)


@dataclass
class SchemaInfo:
    """Schema metadata."""

    name: str
    engine: str
    tables: list[TableInfo] = field(default_factory=list)
    table_count: int = 0
    view_count: int = 0
    total_size_bytes: int = 0
    introspected_at: str = ""
    extra_metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.refresh_derived_fields()

    def refresh_derived_fields(self) -> None:
        if self.tables:
            for table in self.tables:
                table.refresh_derived_fields()
        if self.table_count == 0 and self.tables:
            self.table_count = sum(
                1 for table in self.tables if table.table_type is TableType.TABLE
            )
        if self.view_count == 0 and self.tables:
            self.view_count = len(self.tables) - self.table_count
        if self.total_size_bytes == 0 and self.tables:
            self.total_size_bytes = sum(table.size_bytes for table in self.tables)

    def get_table(self, name: str) -> TableInfo | None:
        for table in self.tables:
            if table.name == name:
                return table
        return None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["tables"] = [table.to_dict() for table in self.tables]
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchemaInfo:
        payload = {name: value for name, value in data.items() if name in cls.__dataclass_fields__}
        payload["tables"] = [TableInfo.from_dict(item) for item in data.get("tables", [])]
        return cls(**payload)


@dataclass
class IntrospectionResult:
    """Full metadata extraction result for a database."""

    database: str
    engine: str
    host: str
    schemas: list[SchemaInfo] = field(default_factory=list)
    total_tables: int = 0
    total_views: int = 0
    total_columns: int = 0
    total_size_bytes: int = 0
    introspected_at: str = ""
    fk_in_degree_map: dict[str, list[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._compute_summary()
        self._apply_fk_in_degree()

    def _compute_summary(self) -> None:
        for schema in self.schemas:
            schema.refresh_derived_fields()
        self.total_tables = sum(schema.table_count for schema in self.schemas)
        self.total_views = sum(schema.view_count for schema in self.schemas)
        self.total_columns = sum(
            len(table.columns) for schema in self.schemas for table in schema.tables
        )
        self.total_size_bytes = sum(schema.total_size_bytes for schema in self.schemas)

    def _apply_fk_in_degree(self) -> None:
        for table in self.all_tables():
            table._fk_in_degree = len(self.fk_in_degree_map.get(table.qualified_name, []))

    def get_schema(self, name: str) -> SchemaInfo | None:
        for schema in self.schemas:
            if schema.name == name:
                return schema
        return None

    def get_table(self, schema: str, table: str) -> TableInfo | None:
        schema_info = self.get_schema(schema)
        if schema_info is None:
            return None
        return schema_info.get_table(table)

    def all_tables(self) -> list[TableInfo]:
        return [table for schema in self.schemas for table in schema.tables]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["schemas"] = [schema.to_dict() for schema in self.schemas]
        return payload

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> IntrospectionResult:
        payload = {name: value for name, value in data.items() if name in cls.__dataclass_fields__}
        payload["schemas"] = [SchemaInfo.from_dict(item) for item in data.get("schemas", [])]
        return cls(**payload)

    @classmethod
    def from_json(cls, payload: str) -> IntrospectionResult:
        return cls.from_dict(json.loads(payload))
