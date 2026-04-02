"""Integration tests for the generic SQLAlchemy connector in Phase 13C."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors import get_connector
from atlas.connectors.generic import SQLAlchemyConnector
from tests.integration.phase_13.helpers import build_phase13_sqlite_fixture

pytestmark = [pytest.mark.integration, pytest.mark.phase_13c]

SQLALCHEMY_AVAILABLE = importlib.util.find_spec("sqlalchemy") is not None


def _generic_sqlite_url(db_path: Path) -> str:
    return f"generic+sqlite:////{db_path.as_posix().lstrip('/')}"


def test_generic_from_url_preserves_sqlalchemy_url_for_sqlite(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "generic_config.sqlite"
    build_phase13_sqlite_fixture(db_path)

    config = AtlasConnectionConfig.from_url(_generic_sqlite_url(db_path))

    assert config.engine is DatabaseEngine.generic
    assert config.host == ""
    assert config.connect_args["sqlalchemy_url"] == f"sqlite:////{db_path.as_posix().lstrip('/')}"
    assert config.connection_string_safe == _generic_sqlite_url(db_path)


def test_generic_from_url_keeps_dialect_identity_in_safe_string() -> None:
    config = AtlasConnectionConfig.from_url(
        "generic+cockroachdb://atlas:secret@db.example:26257/app"
    )

    assert config.engine is DatabaseEngine.generic
    assert config.host == "db.example"
    assert config.database == "app"
    assert config.connect_args["sqlalchemy_url"] == "cockroachdb://atlas:secret@db.example:26257/app"
    assert config.connection_string_safe == "generic+cockroachdb://atlas@db.example:26257/app"


def test_generic_factory_returns_sqlalchemy_connector(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "generic_factory.sqlite"
    build_phase13_sqlite_fixture(db_path)

    connector = get_connector(AtlasConnectionConfig.from_url(_generic_sqlite_url(db_path)))

    assert isinstance(connector, SQLAlchemyConnector)


def test_generic_connector_reports_missing_sqlalchemy_helpfully(
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = phase_tmp_dir / "generic_missing.sqlite"
    build_phase13_sqlite_fixture(db_path)
    connector = SQLAlchemyConnector(AtlasConnectionConfig.from_url(_generic_sqlite_url(db_path)))

    monkeypatch.setattr(
        "atlas.connectors.generic._require_sqlalchemy",
        lambda: (_ for _ in ()).throw(
            ImportError(
                "SQLAlchemy support is not installed. Run 'pip install \"atlas-datamap[generic]\"' or install 'sqlalchemy'."
            )
        ),
    )

    with pytest.raises(ImportError, match="SQLAlchemy support is not installed"):
        connector.connect()


@pytest.mark.skipif(not SQLALCHEMY_AVAILABLE, reason="sqlalchemy is not installed")
def test_generic_sqlalchemy_connector_introspects_sqlite_fixture(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "generic_live.sqlite"
    build_phase13_sqlite_fixture(db_path)
    connector = SQLAlchemyConnector(AtlasConnectionConfig.from_url(_generic_sqlite_url(db_path)))

    with connector.session():
        result = connector.introspect_all()

    assert result.engine == "generic"
    assert result.total_tables >= 4
    assert result.get_schema("main") is not None
    assert result.get_table("main", "authors") is not None
    assert result.get_table("main", "active_authors") is not None


@pytest.mark.skipif(not SQLALCHEMY_AVAILABLE, reason="sqlalchemy is not installed")
def test_generic_sqlalchemy_connector_degrades_physical_metrics_to_zero(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "generic_metrics.sqlite"
    build_phase13_sqlite_fixture(db_path)
    connector = SQLAlchemyConnector(AtlasConnectionConfig.from_url(_generic_sqlite_url(db_path)))

    with connector.session():
        table = next(table for table in connector.get_tables("main") if table.name == "authors")
        schema = connector.introspect_schema("main")

    assert connector.get_row_count_estimate("main", table.name) == 0
    assert connector.get_table_size_bytes("main", table.name) == 0
    assert schema.total_size_bytes == 0


@pytest.mark.skipif(not SQLALCHEMY_AVAILABLE, reason="sqlalchemy is not installed")
def test_generic_connector_logs_degraded_metrics_warning(
    phase_tmp_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    db_path = phase_tmp_dir / "generic_warning.sqlite"
    build_phase13_sqlite_fixture(db_path)
    connector = SQLAlchemyConnector(AtlasConnectionConfig.from_url(_generic_sqlite_url(db_path)))

    with caplog.at_level(logging.WARNING, logger="atlas.connectors.generic"):
        connector.connect()
        connector.disconnect()

    assert "Generic SQLAlchemy connector active" in caplog.text
    assert "uniform node sizing" in caplog.text


@pytest.mark.skipif(not SQLALCHEMY_AVAILABLE, reason="sqlalchemy is not installed")
def test_generic_sqlalchemy_connector_samples_rows_best_effort(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "generic_samples.sqlite"
    build_phase13_sqlite_fixture(db_path)
    config = AtlasConnectionConfig.from_url(_generic_sqlite_url(db_path))
    config.privacy_mode = PrivacyMode.masked
    connector = SQLAlchemyConnector(config)

    with connector.session():
        rows = connector.get_sample_rows("main", "authors", limit=2)

    assert len(rows) == 2
    assert all(row["email"] == "***" for row in rows)
