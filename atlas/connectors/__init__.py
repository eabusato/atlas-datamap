"""Connector factory and public connector exports."""

from __future__ import annotations

import importlib
from typing import Any, cast

from atlas.config import AtlasConnectionConfig, DatabaseEngine
from atlas.connectors.base import BaseConnector


def get_connector(config: AtlasConnectionConfig) -> BaseConnector:
    """Instantiate the connector registered for the configured engine."""
    if config.engine is DatabaseEngine.postgresql:
        try:
            module = importlib.import_module("atlas.connectors.postgresql")
        except ImportError as exc:
            raise ImportError(
                "PostgreSQL support is not installed. Run 'pip install \"atlas-datamap[postgresql]\"' "
                "or install 'psycopg2-binary'."
            ) from exc
        connector_class = cast(type[BaseConnector], cast(Any, module).PostgreSQLConnector)
        return connector_class(config)

    if config.engine is DatabaseEngine.mysql:
        try:
            module = importlib.import_module("atlas.connectors.mysql")
        except ImportError as exc:
            raise ImportError(
                "MySQL support is not installed. Run 'pip install \"atlas-datamap[mysql]\"' "
                "or install 'mysql-connector-python'."
            ) from exc
        connector_class = cast(type[BaseConnector], cast(Any, module).MySQLConnector)
        return connector_class(config)

    if config.engine is DatabaseEngine.mssql:
        try:
            module = importlib.import_module("atlas.connectors.mssql")
        except ImportError as exc:
            raise ImportError(
                "SQL Server support is not installed. Run 'pip install \"atlas-datamap[mssql]\"' "
                "or install 'pyodbc'."
            ) from exc
        connector_class = cast(type[BaseConnector], cast(Any, module).MSSQLConnector)
        return connector_class(config)

    if config.engine is DatabaseEngine.sqlite:
        from atlas.connectors.sqlite import SQLiteConnector

        return SQLiteConnector(config)

    if config.engine is DatabaseEngine.generic:
        from atlas.connectors.generic import SQLAlchemyConnector

        return SQLAlchemyConnector(config)

    raise ValueError(
        f"Unsupported engine {config.engine.value!r}. Supported values: "
        f"{', '.join(engine.value for engine in DatabaseEngine)}."
    )


__all__ = ["BaseConnector", "get_connector"]
