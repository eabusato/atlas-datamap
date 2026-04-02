"""Canonical native-type normalization shared by Atlas connectors."""

from __future__ import annotations

import re
from collections import Counter

from atlas.types import AtlasType

EngineStr = str

_POSTGRESQL_TYPE_MAP: dict[str, AtlasType] = {
    "smallint": AtlasType.SMALLINT,
    "int2": AtlasType.SMALLINT,
    "integer": AtlasType.INTEGER,
    "int": AtlasType.INTEGER,
    "int4": AtlasType.INTEGER,
    "bigint": AtlasType.BIGINT,
    "int8": AtlasType.BIGINT,
    "serial": AtlasType.INTEGER,
    "bigserial": AtlasType.BIGINT,
    "smallserial": AtlasType.SMALLINT,
    "real": AtlasType.FLOAT,
    "float4": AtlasType.FLOAT,
    "double precision": AtlasType.DOUBLE,
    "float8": AtlasType.DOUBLE,
    "float": AtlasType.FLOAT,
    "numeric": AtlasType.DECIMAL,
    "decimal": AtlasType.DECIMAL,
    "money": AtlasType.MONEY,
    "character varying": AtlasType.TEXT,
    "varchar": AtlasType.TEXT,
    "character": AtlasType.CHAR,
    "char": AtlasType.CHAR,
    "bpchar": AtlasType.CHAR,
    "text": AtlasType.CLOB,
    "name": AtlasType.TEXT,
    "citext": AtlasType.TEXT,
    "boolean": AtlasType.BOOLEAN,
    "bool": AtlasType.BOOLEAN,
    "date": AtlasType.DATE,
    "time": AtlasType.TIME,
    "time without time zone": AtlasType.TIME,
    "time with time zone": AtlasType.TIME,
    "timetz": AtlasType.TIME,
    "timestamp": AtlasType.DATETIME,
    "timestamp without time zone": AtlasType.DATETIME,
    "timestamp with time zone": AtlasType.TIMESTAMP,
    "timestamptz": AtlasType.TIMESTAMP,
    "interval": AtlasType.INTERVAL,
    "bytea": AtlasType.BINARY,
    "json": AtlasType.JSON,
    "jsonb": AtlasType.JSON,
    "xml": AtlasType.XML,
    "uuid": AtlasType.UUID,
    "array": AtlasType.ARRAY,
    "anyarray": AtlasType.ARRAY,
    "geometry": AtlasType.SPATIAL,
    "geography": AtlasType.SPATIAL,
    "point": AtlasType.SPATIAL,
    "line": AtlasType.SPATIAL,
    "lseg": AtlasType.SPATIAL,
    "box": AtlasType.SPATIAL,
    "path": AtlasType.SPATIAL,
    "polygon": AtlasType.SPATIAL,
    "circle": AtlasType.SPATIAL,
    "oid": AtlasType.INTEGER,
    "xid": AtlasType.INTEGER,
    "cid": AtlasType.INTEGER,
    "tid": AtlasType.UNKNOWN,
    "inet": AtlasType.TEXT,
    "cidr": AtlasType.TEXT,
    "macaddr": AtlasType.TEXT,
    "macaddr8": AtlasType.TEXT,
    "tsvector": AtlasType.TEXT,
    "tsquery": AtlasType.TEXT,
    "bit": AtlasType.BINARY,
    "bit varying": AtlasType.BINARY,
    "varbit": AtlasType.BINARY,
    "hstore": AtlasType.JSON,
    "ltree": AtlasType.TEXT,
}

_MYSQL_TYPE_MAP: dict[str, AtlasType] = {
    "tinyint": AtlasType.TINYINT,
    "smallint": AtlasType.SMALLINT,
    "mediumint": AtlasType.INTEGER,
    "int": AtlasType.INTEGER,
    "integer": AtlasType.INTEGER,
    "bigint": AtlasType.BIGINT,
    "float": AtlasType.FLOAT,
    "double": AtlasType.DOUBLE,
    "double precision": AtlasType.DOUBLE,
    "real": AtlasType.FLOAT,
    "decimal": AtlasType.DECIMAL,
    "dec": AtlasType.DECIMAL,
    "numeric": AtlasType.DECIMAL,
    "fixed": AtlasType.DECIMAL,
    "varchar": AtlasType.TEXT,
    "char": AtlasType.CHAR,
    "tinytext": AtlasType.TEXT,
    "text": AtlasType.CLOB,
    "mediumtext": AtlasType.CLOB,
    "longtext": AtlasType.CLOB,
    "bool": AtlasType.BOOLEAN,
    "boolean": AtlasType.BOOLEAN,
    "date": AtlasType.DATE,
    "time": AtlasType.TIME,
    "datetime": AtlasType.DATETIME,
    "timestamp": AtlasType.TIMESTAMP,
    "year": AtlasType.DATE,
    "binary": AtlasType.BINARY,
    "varbinary": AtlasType.BINARY,
    "tinyblob": AtlasType.BINARY,
    "blob": AtlasType.BINARY,
    "mediumblob": AtlasType.BINARY,
    "longblob": AtlasType.BINARY,
    "json": AtlasType.JSON,
    "enum": AtlasType.ENUM,
    "set": AtlasType.ENUM,
    "geometry": AtlasType.SPATIAL,
    "point": AtlasType.SPATIAL,
    "linestring": AtlasType.SPATIAL,
    "polygon": AtlasType.SPATIAL,
    "multipoint": AtlasType.SPATIAL,
    "multilinestring": AtlasType.SPATIAL,
    "multipolygon": AtlasType.SPATIAL,
    "geometrycollection": AtlasType.SPATIAL,
}

_MSSQL_TYPE_MAP: dict[str, AtlasType] = {
    "tinyint": AtlasType.TINYINT,
    "smallint": AtlasType.SMALLINT,
    "int": AtlasType.INTEGER,
    "integer": AtlasType.INTEGER,
    "bigint": AtlasType.BIGINT,
    "float": AtlasType.FLOAT,
    "real": AtlasType.FLOAT,
    "decimal": AtlasType.DECIMAL,
    "numeric": AtlasType.DECIMAL,
    "money": AtlasType.MONEY,
    "smallmoney": AtlasType.MONEY,
    "char": AtlasType.CHAR,
    "varchar": AtlasType.TEXT,
    "text": AtlasType.CLOB,
    "nchar": AtlasType.CHAR,
    "nvarchar": AtlasType.TEXT,
    "ntext": AtlasType.CLOB,
    "sysname": AtlasType.TEXT,
    "bit": AtlasType.BOOLEAN,
    "date": AtlasType.DATE,
    "time": AtlasType.TIME,
    "datetime": AtlasType.DATETIME,
    "datetime2": AtlasType.DATETIME,
    "datetimeoffset": AtlasType.TIMESTAMP,
    "smalldatetime": AtlasType.DATETIME,
    "binary": AtlasType.BINARY,
    "varbinary": AtlasType.BINARY,
    "image": AtlasType.BINARY,
    "timestamp": AtlasType.BINARY,
    "rowversion": AtlasType.BINARY,
    "xml": AtlasType.XML,
    "uniqueidentifier": AtlasType.UUID,
    "geometry": AtlasType.SPATIAL,
    "geography": AtlasType.SPATIAL,
    "hierarchyid": AtlasType.TEXT,
    "sql_variant": AtlasType.UNKNOWN,
    "cursor": AtlasType.UNKNOWN,
    "table": AtlasType.UNKNOWN,
}

_ENGINE_MAPS: dict[str, dict[str, AtlasType]] = {
    "postgresql": _POSTGRESQL_TYPE_MAP,
    "mysql": _MYSQL_TYPE_MAP,
    "mariadb": _MYSQL_TYPE_MAP,
    "mssql": _MSSQL_TYPE_MAP,
    "sqlserver": _MSSQL_TYPE_MAP,
}


def _extract_base_type(native_type: str) -> str:
    """Strip width, precision, unsigned modifiers, and PostgreSQL array suffixes."""
    if not native_type:
        return ""
    normalized = native_type.strip().lower()
    normalized = re.sub(r"\[\]$", "", normalized).strip()
    normalized = re.sub(r"\s*\([^)]*\)", "", normalized).strip()
    normalized = re.sub(r"\s+unsigned$", "", normalized).strip()
    return normalized


def _mysql_special_cases(native_type: str) -> AtlasType | None:
    normalized = native_type.strip().lower()
    if normalized in {"tinyint(1)", "bool", "boolean", "bit(1)"}:
        return AtlasType.BOOLEAN
    return None


def _mariadb_special_cases(native_type: str) -> AtlasType | None:
    normalized = native_type.strip().lower()
    if normalized in {"inet4", "inet6"}:
        return AtlasType.TEXT
    if normalized == "uuid":
        return AtlasType.UUID
    return None


def _postgresql_special_cases(native_type: str) -> AtlasType | None:
    normalized = native_type.strip().lower()
    if normalized == "user-defined":
        return AtlasType.UNKNOWN
    if normalized.endswith("[]"):
        return AtlasType.ARRAY
    return None


def normalize_type(native_type: str, engine: EngineStr) -> AtlasType:
    """Map a native engine type name to the canonical AtlasType vocabulary."""
    if not native_type:
        return AtlasType.UNKNOWN

    engine_key = engine.lower().strip()
    if engine_key in {"mysql", "mariadb"}:
        special = _mysql_special_cases(native_type)
        if special is not None:
            return special
    if engine_key == "mariadb":
        special = _mariadb_special_cases(native_type)
        if special is not None:
            return special

    if engine_key == "postgresql":
        special = _postgresql_special_cases(native_type)
        if special is not None:
            return special

    base = _extract_base_type(native_type)
    type_map = _ENGINE_MAPS.get(engine_key, {})
    if base in type_map:
        return type_map[base]

    first_word = base.split()[0] if base else ""
    if first_word in type_map:
        return type_map[first_word]
    return AtlasType.UNKNOWN


def list_unmapped_types(types: list[str], engine: EngineStr) -> list[str]:
    """Return the raw native types that still normalize to UNKNOWN."""
    return [native_type for native_type in types if normalize_type(native_type, engine) is AtlasType.UNKNOWN]


def get_type_coverage(engine: EngineStr) -> dict[str, int | dict[str, int]]:
    """Return mapping-cardinality stats for the given engine."""
    type_map = _ENGINE_MAPS.get(engine.lower().strip(), {})
    counter: Counter[str] = Counter()
    for atlas_type in type_map.values():
        counter[atlas_type.value] += 1
    return {
        "total": len(type_map),
        "per_category": dict(counter),
    }
