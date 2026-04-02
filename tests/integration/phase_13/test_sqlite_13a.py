"""Integration tests for the Phase 13A SQLite connector."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors import get_connector
from atlas.connectors.sqlite import SQLiteConnector
from atlas.types import AtlasType, TableType
from tests.integration.phase_13.helpers import build_phase13_sqlite_fixture

pytestmark = [pytest.mark.integration, pytest.mark.phase_13a]


def _sqlite_config(db_path: Path, *, privacy_mode: PrivacyMode = PrivacyMode.normal) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(
        f"sqlite:///{db_path.as_posix()}",
        privacy_mode=privacy_mode,
    )


def test_sqlite_introspection_reads_tables_views_and_schema_size(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "phase13.sqlite"
    build_phase13_sqlite_fixture(db_path)

    connector = get_connector(_sqlite_config(db_path))
    with connector.session():
        result = connector.introspect_all()

    assert result.engine == "sqlite"
    assert result.total_tables == 4
    assert result.total_views == 1
    schema = result.get_schema("main")
    assert schema is not None
    assert schema.total_size_bytes == db_path.stat().st_size
    assert schema.table_count == 4
    assert schema.view_count == 1


def test_sqlite_marks_views_correctly(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "views.sqlite"
    build_phase13_sqlite_fixture(db_path)

    connector = SQLiteConnector(_sqlite_config(db_path))
    with connector.session():
        tables = connector.get_tables("main")

    active_authors = next(table for table in tables if table.name == "active_authors")
    assert active_authors.table_type is TableType.VIEW


def test_sqlite_maps_affinity_types_to_atlas_types(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "types.sqlite"
    build_phase13_sqlite_fixture(db_path)

    connector = SQLiteConnector(_sqlite_config(db_path))
    with connector.session():
        columns = {column.name: column for column in connector.get_columns("main", "authors")}

    assert columns["id"].canonical_type is AtlasType.INTEGER
    assert columns["email"].canonical_type in {AtlasType.CLOB, AtlasType.TEXT}
    assert columns["active"].canonical_type is AtlasType.BOOLEAN
    assert columns["profile_json"].canonical_type is AtlasType.JSON
    assert columns["birth_date"].canonical_type is AtlasType.DATE
    assert columns["created_at"].canonical_type is AtlasType.DATETIME


def test_sqlite_groups_composite_foreign_keys_into_single_constraint(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "composite_fk.sqlite"
    build_phase13_sqlite_fixture(db_path)

    connector = SQLiteConnector(_sqlite_config(db_path))
    with connector.session():
        foreign_keys = connector.get_foreign_keys("main", "membership_audit")

    assert len(foreign_keys) == 1
    foreign_key = foreign_keys[0]
    assert foreign_key.target_schema == "main"
    assert foreign_key.target_table == "memberships"
    assert foreign_key.source_columns == ["user_id", "team_id"]
    assert foreign_key.target_columns == ["user_id", "team_id"]


def test_sqlite_extracts_explicit_and_partial_indexes(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "indexes.sqlite"
    build_phase13_sqlite_fixture(db_path)

    connector = SQLiteConnector(_sqlite_config(db_path))
    with connector.session():
        indexes = {index.name: index for index in connector.get_indexes("main", "books")}

    assert "idx_books_author_title" in indexes
    assert indexes["idx_books_author_title"].columns == ["author_id", "title"]
    assert indexes["idx_books_author_title"].is_unique is False
    assert "idx_books_price_positive" in indexes
    assert indexes["idx_books_price_positive"].is_partial is True


def test_sqlite_sampling_respects_masked_privacy_mode(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "masked.sqlite"
    build_phase13_sqlite_fixture(db_path)

    connector = SQLiteConnector(_sqlite_config(db_path, privacy_mode=PrivacyMode.masked))
    with connector.session():
        rows = connector.get_sample_rows("main", "authors")

    assert len(rows) == 2
    assert rows[0]["email"] == "***"
    assert rows[0]["display_name"] in {"Alice", "Bob"}


def test_sqlite_column_stats_use_real_counts(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "stats.sqlite"
    build_phase13_sqlite_fixture(db_path)

    connector = SQLiteConnector(_sqlite_config(db_path))
    with connector.session():
        row_count = connector.get_row_count_estimate("main", "books")
        null_count = connector.get_column_null_count("main", "books", "price")
        distinct_count = connector.get_column_distinct_estimate("main", "books", "author_id")

    assert row_count == 3
    assert null_count == 1
    assert distinct_count == 2


def test_sqlite_factory_still_resolves_standard_engine(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "factory.sqlite"
    build_phase13_sqlite_fixture(db_path)

    connector = get_connector(_sqlite_config(db_path))
    assert isinstance(connector, SQLiteConnector)
    assert connector.config.engine is DatabaseEngine.sqlite
