"""Integration tests for MariaDB differentiation in Phase 13B."""

from __future__ import annotations

from typing import Any

import pytest

from atlas.connectors.mysql import MySQLConnector
from atlas.connectors.type_mapping import normalize_type
from atlas.types import AtlasType, IntrospectionResult

pytestmark = [pytest.mark.integration, pytest.mark.phase_13b]


def _ensure_mariadb_phase13_objects(mysql_driver: Any) -> None:
    connection = mysql_driver.connect(
        host="127.0.0.1",
        port=3308,
        user="atlas",
        password="atlas_pass",
        database="atlas_test",
        autocommit=True,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("DROP FUNCTION IF EXISTS fn_customer_count")
            cursor.execute(
                """
                CREATE FUNCTION fn_customer_count()
                RETURNS INT
                DETERMINISTIC
                COMMENT 'Counts customers for Atlas tests'
                RETURN 1
                """
            )
            cursor.execute("DROP SEQUENCE IF EXISTS seq_phase13_invoice")
            cursor.execute("CREATE SEQUENCE seq_phase13_invoice START WITH 100 INCREMENT BY 1")
    finally:
        connection.close()


def test_mariadb_introspection_marks_result_as_mariadb(
    mariadb_connector: MySQLConnector,
    mysql_driver: Any,
) -> None:
    _ensure_mariadb_phase13_objects(mysql_driver)

    result = mariadb_connector.introspect_all()

    assert result.engine == "mariadb"
    assert result.schemas
    assert all(schema.engine == "mariadb" for schema in result.schemas)


def test_mariadb_schema_extra_metadata_contains_routines_and_sequences(
    mariadb_connector: MySQLConnector,
    mysql_driver: Any,
) -> None:
    _ensure_mariadb_phase13_objects(mysql_driver)

    result = mariadb_connector.introspect_all()
    schema = next(schema for schema in result.schemas if schema.name == "atlas_test")

    assert "mariadb_routines" in schema.extra_metadata
    assert "mariadb_sequences" in schema.extra_metadata
    routine_names = {item["name"] for item in schema.extra_metadata["mariadb_routines"]}
    assert "fn_customer_count" in routine_names
    assert isinstance(schema.extra_metadata["mariadb_sequences"], list)


def test_mariadb_extra_metadata_survives_result_serialization(
    mariadb_connector: MySQLConnector,
    mysql_driver: Any,
) -> None:
    _ensure_mariadb_phase13_objects(mysql_driver)

    result = mariadb_connector.introspect_all()
    roundtrip = IntrospectionResult.from_json(result.to_json())
    schema = next(schema for schema in roundtrip.schemas if schema.name == "atlas_test")

    assert schema.engine == "mariadb"
    assert "fn_customer_count" in {
        item["name"] for item in schema.extra_metadata["mariadb_routines"]
    }
    assert isinstance(schema.extra_metadata["mariadb_sequences"], list)


def test_mariadb_specific_type_mapping_is_supported() -> None:
    assert normalize_type("json", "mariadb") is AtlasType.JSON
    assert normalize_type("inet4", "mariadb") is AtlasType.TEXT
    assert normalize_type("inet6", "mariadb") is AtlasType.TEXT
    assert normalize_type("uuid", "mariadb") is AtlasType.UUID


def test_mariadb_live_columns_keep_json_as_canonical_json(
    mariadb_connector: MySQLConnector,
) -> None:
    columns = {
        column.name: column for column in mariadb_connector.get_columns("atlas_test", "products")
    }

    assert columns["metadata"].canonical_type is AtlasType.JSON


def test_mariadb_sequence_lookup_degrades_gracefully_when_query_fails(
    mariadb_connector: MySQLConnector,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        mariadb_connector,
        "_fetchall",
        lambda sql, params: (_ for _ in ()).throw(RuntimeError("broken sequences")),
    )

    sequences = mariadb_connector._get_mariadb_sequences("atlas_test")

    assert sequences == []
