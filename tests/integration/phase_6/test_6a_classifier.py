"""Phase 6A integration tests for heuristic table classification."""

from __future__ import annotations

import pytest

from atlas.analysis import TableClassifier
from tests.integration.phase_6.helpers import (
    build_analysis_sqlite_fixture,
    introspect_analysis_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_6a]


def test_classifier_detects_staging_table_from_real_sqlite_fixture(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)

    classifications = TableClassifier().classify_all(result)
    by_name = {item.table: item for item in classifications}

    assert by_name["stg_orders_raw"].probable_type == "staging"
    assert result.get_table("main", "stg_orders_raw").heuristic_type == "staging"


def test_classifier_detects_config_table_from_real_sqlite_fixture(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)

    classifications = TableClassifier().classify_all(result)
    by_name = {item.table: item for item in classifications}

    assert by_name["customer_settings"].probable_type == "config"


def test_classifier_detects_pivot_and_fact_tables_from_real_sqlite_fixture(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)

    classifications = TableClassifier().classify_all(result)
    by_name = {item.table: item for item in classifications}

    assert by_name["order_tags"].probable_type == "pivot"
    assert by_name["fact_sales"].probable_type == "fact"


def test_classifier_detects_log_table_from_real_sqlite_fixture(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)

    classifications = TableClassifier().classify_all(result)
    by_name = {item.table: item for item in classifications}

    assert by_name["audit_log"].probable_type == "log"


def test_classifier_detects_domain_and_dimension_tables_from_real_sqlite_fixture(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)

    classifications = TableClassifier().classify_all(result)
    by_name = {item.table: item for item in classifications}

    assert by_name["customers"].probable_type == "domain_main"
    assert by_name["dim_products"].probable_type == "dimension"
