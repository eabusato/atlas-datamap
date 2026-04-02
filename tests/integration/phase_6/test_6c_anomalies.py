"""Phase 6C integration tests for structural anomaly detection."""

from __future__ import annotations

import pytest

from atlas.analysis import AnomalyDetector, TableClassifier
from tests.integration.phase_6.helpers import (
    build_analysis_sqlite_fixture,
    introspect_analysis_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_6c]


def test_detector_finds_missing_keys_and_indexes_in_real_fixture(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)
    TableClassifier().classify_all(result)

    anomalies = AnomalyDetector().detect(result)
    unresolved = [item for item in anomalies if item.table == "unresolved_links"]
    unresolved_types = {item.anomaly_type for item in unresolved}

    assert {"no_indexes", "no_pk", "high_nullable_no_pk", "implicit_fk", "ambiguous_column_name"} <= unresolved_types


def test_detector_reports_fk_without_index_in_real_fixture(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)

    anomalies = AnomalyDetector().detect(result)

    assert any(
        item.table == "customer_settings" and item.anomaly_type == "fk_without_index"
        for item in anomalies
    )


def test_detector_reports_empty_and_wide_tables_in_real_fixture(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)
    TableClassifier().classify_all(result)

    anomalies = AnomalyDetector().detect(result)

    assert any(item.table == "empty_archive" and item.anomaly_type == "empty_table" for item in anomalies)
    assert any(item.table == "wide_metrics" and item.anomaly_type == "wide_table" for item in anomalies)


def test_detector_does_not_flag_view_for_missing_pk_or_indexes(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)

    anomalies = AnomalyDetector().detect(result)
    view_anomalies = [item for item in anomalies if item.table == "customer_emails"]

    assert not any(item.anomaly_type in {"no_indexes", "no_pk"} for item in view_anomalies)


def test_detector_summarizes_real_fixture_anomalies(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)
    anomalies = AnomalyDetector().detect(result)

    summary = AnomalyDetector().summarize(anomalies)

    assert summary["no_indexes"] >= 1
    assert summary["fk_without_index"] >= 1
