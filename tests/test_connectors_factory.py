"""Unit tests for connector factory behavior."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine
from atlas.connectors import get_connector
from atlas.connectors.generic import SQLAlchemyConnector
from atlas.connectors.mssql import MSSQLConnector
from atlas.connectors.mysql import MySQLConnector
from atlas.connectors.postgresql import PostgreSQLConnector
from atlas.connectors.sqlite import SQLiteConnector


def _config(engine: DatabaseEngine) -> AtlasConnectionConfig:
    if engine is DatabaseEngine.sqlite:
        return AtlasConnectionConfig(engine=engine, host="", database=":memory:")
    if engine is DatabaseEngine.generic:
        return AtlasConnectionConfig(
            engine=engine,
            host="",
            database="/tmp/atlas.db",
            connect_args={"sqlalchemy_url": "sqlite:////tmp/atlas.db"},
        )
    return AtlasConnectionConfig(engine=engine, host="localhost", database="atlas")


def test_sqlite_factory_returns_sqlite_connector() -> None:
    connector = get_connector(_config(DatabaseEngine.sqlite))
    assert isinstance(connector, SQLiteConnector)


def test_generic_factory_returns_sqlalchemy_connector() -> None:
    connector = get_connector(_config(DatabaseEngine.generic))
    assert isinstance(connector, SQLAlchemyConnector)


def test_postgresql_factory_returns_postgresql_connector() -> None:
    connector = get_connector(_config(DatabaseEngine.postgresql))
    assert isinstance(connector, PostgreSQLConnector)


@pytest.mark.parametrize(
    ("engine", "connector_type"),
    [
        (DatabaseEngine.mysql, MySQLConnector),
        (DatabaseEngine.mssql, MSSQLConnector),
    ],
)
def test_factory_returns_real_optional_connectors_when_installed(
    engine: DatabaseEngine,
    connector_type: type[object],
) -> None:
    connector = get_connector(_config(engine))
    assert isinstance(connector, connector_type)


@pytest.mark.parametrize(
    ("engine", "expected_message"),
    [
        (DatabaseEngine.mysql, "MySQL support is not installed"),
        (DatabaseEngine.mssql, "SQL Server support is not installed"),
    ],
)
def test_missing_optional_driver_raises_helpful_import_error(
    engine: DatabaseEngine,
    expected_message: str,
) -> None:
    with (
        patch("atlas.connectors.importlib.import_module", side_effect=ImportError("missing")),
        pytest.raises(ImportError, match=expected_message),
    ):
        get_connector(_config(engine))
