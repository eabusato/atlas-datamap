"""Unit tests for shared native-type normalization."""

from __future__ import annotations

import pytest

from atlas.connectors.type_mapping import (
    _MSSQL_TYPE_MAP,
    _MYSQL_TYPE_MAP,
    _POSTGRESQL_TYPE_MAP,
    _extract_base_type,
    get_type_coverage,
    normalize_type,
)
from atlas.types import AtlasType


class TestExtractBaseType:
    @pytest.mark.parametrize(
        ("native_type", "expected"),
        [
            ("int", "int"),
            ("varchar(255)", "varchar"),
            ("decimal(10, 2)", "decimal"),
            ("numeric(18,4)", "numeric"),
            ("int unsigned", "int"),
            ("bigint unsigned", "bigint"),
            ("int[]", "int"),
            ("text[]", "text"),
            ("uuid[]", "uuid"),
            ("character varying(100)", "character varying"),
            ("double precision", "double precision"),
            ("timestamp without time zone", "timestamp without time zone"),
            ("timestamp with time zone", "timestamp with time zone"),
            ("nvarchar(max)", "nvarchar"),
            ("VARCHAR(100)", "varchar"),
            ("  int  ", "int"),
            ("", ""),
        ],
    )
    def test_extract_base_type(self, native_type: str, expected: str) -> None:
        assert _extract_base_type(native_type) == expected


class TestPostgresqlMapping:
    @pytest.mark.parametrize(
        ("native_type", "expected"),
        [
            ("integer", AtlasType.INTEGER),
            ("int4", AtlasType.INTEGER),
            ("bigint", AtlasType.BIGINT),
            ("int8", AtlasType.BIGINT),
            ("smallint", AtlasType.SMALLINT),
            ("int2", AtlasType.SMALLINT),
            ("serial", AtlasType.INTEGER),
            ("bigserial", AtlasType.BIGINT),
            ("character varying(255)", AtlasType.TEXT),
            ("varchar(100)", AtlasType.TEXT),
            ("text", AtlasType.CLOB),
            ("character(10)", AtlasType.CHAR),
            ("boolean", AtlasType.BOOLEAN),
            ("bool", AtlasType.BOOLEAN),
            ("date", AtlasType.DATE),
            ("time", AtlasType.TIME),
            ("timestamp", AtlasType.DATETIME),
            ("timestamp without time zone", AtlasType.DATETIME),
            ("timestamp with time zone", AtlasType.TIMESTAMP),
            ("timestamptz", AtlasType.TIMESTAMP),
            ("interval", AtlasType.INTERVAL),
            ("numeric", AtlasType.DECIMAL),
            ("decimal(10,2)", AtlasType.DECIMAL),
            ("money", AtlasType.MONEY),
            ("json", AtlasType.JSON),
            ("jsonb", AtlasType.JSON),
            ("uuid", AtlasType.UUID),
            ("bytea", AtlasType.BINARY),
            ("int[]", AtlasType.ARRAY),
            ("text[]", AtlasType.ARRAY),
            ("USER-DEFINED", AtlasType.UNKNOWN),
            ("xml", AtlasType.XML),
            ("geometry", AtlasType.SPATIAL),
            ("point", AtlasType.SPATIAL),
            ("real", AtlasType.FLOAT),
            ("double precision", AtlasType.DOUBLE),
            ("inet", AtlasType.TEXT),
            ("cidr", AtlasType.TEXT),
            ("macaddr", AtlasType.TEXT),
            ("hstore", AtlasType.JSON),
        ],
    )
    def test_normalize_type(self, native_type: str, expected: AtlasType) -> None:
        assert normalize_type(native_type, "postgresql") is expected


class TestMysqlMapping:
    @pytest.mark.parametrize(
        ("native_type", "expected"),
        [
            ("tinyint", AtlasType.TINYINT),
            ("smallint", AtlasType.SMALLINT),
            ("mediumint", AtlasType.INTEGER),
            ("int", AtlasType.INTEGER),
            ("integer", AtlasType.INTEGER),
            ("bigint", AtlasType.BIGINT),
            ("int unsigned", AtlasType.INTEGER),
            ("bigint unsigned", AtlasType.BIGINT),
            ("tinyint(1)", AtlasType.BOOLEAN),
            ("bool", AtlasType.BOOLEAN),
            ("boolean", AtlasType.BOOLEAN),
            ("bit(1)", AtlasType.BOOLEAN),
            ("varchar(255)", AtlasType.TEXT),
            ("char(10)", AtlasType.CHAR),
            ("tinytext", AtlasType.TEXT),
            ("text", AtlasType.CLOB),
            ("mediumtext", AtlasType.CLOB),
            ("longtext", AtlasType.CLOB),
            ("date", AtlasType.DATE),
            ("time", AtlasType.TIME),
            ("datetime", AtlasType.DATETIME),
            ("timestamp", AtlasType.TIMESTAMP),
            ("year", AtlasType.DATE),
            ("decimal(10,2)", AtlasType.DECIMAL),
            ("numeric", AtlasType.DECIMAL),
            ("float", AtlasType.FLOAT),
            ("double precision", AtlasType.DOUBLE),
            ("binary", AtlasType.BINARY),
            ("varbinary", AtlasType.BINARY),
            ("blob", AtlasType.BINARY),
            ("longblob", AtlasType.BINARY),
            ("json", AtlasType.JSON),
            ("enum", AtlasType.ENUM),
            ("set", AtlasType.ENUM),
            ("geometry", AtlasType.SPATIAL),
            ("polygon", AtlasType.SPATIAL),
        ],
    )
    def test_normalize_type(self, native_type: str, expected: AtlasType) -> None:
        assert normalize_type(native_type, "mysql") is expected

    @pytest.mark.parametrize(
        ("native_type", "expected"),
        [
            ("int", AtlasType.INTEGER),
            ("tinyint(1)", AtlasType.BOOLEAN),
            ("json", AtlasType.JSON),
        ],
    )
    def test_mariadb_uses_mysql_mapping(self, native_type: str, expected: AtlasType) -> None:
        assert normalize_type(native_type, "mariadb") is expected


class TestMssqlMapping:
    @pytest.mark.parametrize(
        ("native_type", "expected"),
        [
            ("tinyint", AtlasType.TINYINT),
            ("smallint", AtlasType.SMALLINT),
            ("int", AtlasType.INTEGER),
            ("bigint", AtlasType.BIGINT),
            ("bit", AtlasType.BOOLEAN),
            ("varchar(255)", AtlasType.TEXT),
            ("nvarchar(255)", AtlasType.TEXT),
            ("nvarchar(max)", AtlasType.TEXT),
            ("varchar(max)", AtlasType.TEXT),
            ("char(10)", AtlasType.CHAR),
            ("nchar(10)", AtlasType.CHAR),
            ("text", AtlasType.CLOB),
            ("ntext", AtlasType.CLOB),
            ("date", AtlasType.DATE),
            ("time", AtlasType.TIME),
            ("datetime", AtlasType.DATETIME),
            ("datetime2", AtlasType.DATETIME),
            ("datetimeoffset", AtlasType.TIMESTAMP),
            ("smalldatetime", AtlasType.DATETIME),
            ("decimal(18,4)", AtlasType.DECIMAL),
            ("numeric", AtlasType.DECIMAL),
            ("money", AtlasType.MONEY),
            ("smallmoney", AtlasType.MONEY),
            ("binary", AtlasType.BINARY),
            ("varbinary", AtlasType.BINARY),
            ("image", AtlasType.BINARY),
            ("timestamp", AtlasType.BINARY),
            ("rowversion", AtlasType.BINARY),
            ("uniqueidentifier", AtlasType.UUID),
            ("xml", AtlasType.XML),
            ("geometry", AtlasType.SPATIAL),
            ("geography", AtlasType.SPATIAL),
            ("sql_variant", AtlasType.UNKNOWN),
            ("float(53)", AtlasType.FLOAT),
            ("real", AtlasType.FLOAT),
        ],
    )
    def test_normalize_type(self, native_type: str, expected: AtlasType) -> None:
        assert normalize_type(native_type, "mssql") is expected

    def test_sqlserver_alias(self) -> None:
        assert normalize_type("int", "sqlserver") is AtlasType.INTEGER
        assert normalize_type("uniqueidentifier", "sqlserver") is AtlasType.UUID


class TestGenericBehavior:
    def test_empty_type_returns_unknown(self) -> None:
        for engine in ("postgresql", "mysql", "mssql"):
            assert normalize_type("", engine) is AtlasType.UNKNOWN

    def test_unknown_engine_returns_unknown(self) -> None:
        assert normalize_type("int", "oracle") is AtlasType.UNKNOWN

    @pytest.mark.parametrize(
        ("native_type", "engine"),
        [
            ("SOME_CUSTOM_TYPE", "postgresql"),
            ("", "mysql"),
            ("   ", "mssql"),
            ("custom_enum_type", "postgresql"),
            ("xml[]", "postgresql"),
        ],
    )
    def test_never_raises(self, native_type: str, engine: str) -> None:
        normalize_type(native_type, engine)

    def test_case_insensitive_engine(self) -> None:
        assert normalize_type("int", "PostgreSQL") is AtlasType.INTEGER
        assert normalize_type("int", "MYSQL") is AtlasType.INTEGER
        assert normalize_type("int", "MSSQL") is AtlasType.INTEGER

    @pytest.mark.parametrize(
        ("pg_type", "mysql_type", "mssql_type", "expected"),
        [
            ("integer", "int", "int", AtlasType.INTEGER),
            ("character varying", "varchar", "varchar", AtlasType.TEXT),
            ("boolean", "tinyint(1)", "bit", AtlasType.BOOLEAN),
            ("date", "date", "date", AtlasType.DATE),
            ("bytea", "blob", "varbinary", AtlasType.BINARY),
        ],
    )
    def test_cross_engine_consistency(
        self,
        pg_type: str,
        mysql_type: str,
        mssql_type: str,
        expected: AtlasType,
    ) -> None:
        assert normalize_type(pg_type, "postgresql") is expected
        assert normalize_type(mysql_type, "mysql") is expected
        assert normalize_type(mssql_type, "mssql") is expected


class TestCoverage:
    def test_postgresql_map_has_minimum_coverage(self) -> None:
        stats = get_type_coverage("postgresql")
        assert stats["total"] >= 30

    def test_mysql_map_has_minimum_coverage(self) -> None:
        stats = get_type_coverage("mysql")
        assert stats["total"] >= 25

    def test_mssql_map_has_minimum_coverage(self) -> None:
        stats = get_type_coverage("mssql")
        assert stats["total"] >= 25

    def test_all_atlas_types_are_mapped_by_at_least_one_engine(self) -> None:
        all_mapped = set(_POSTGRESQL_TYPE_MAP.values()) | set(_MYSQL_TYPE_MAP.values()) | set(
            _MSSQL_TYPE_MAP.values()
        )
        for atlas_type in AtlasType:
            if atlas_type is AtlasType.UNKNOWN:
                continue
            assert atlas_type in all_mapped

