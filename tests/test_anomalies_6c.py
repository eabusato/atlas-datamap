"""Phase 6C unit tests for structural anomaly detection."""

from __future__ import annotations

from atlas.analysis.anomalies import AnomalyDetector, AnomalySeverity, StructuralAnomaly
from atlas.types import TableType
from tests.phase_6_samples import make_column, make_fk, make_index, make_result, make_table


def test_severity_and_location_contract() -> None:
    anomaly = StructuralAnomaly(
        anomaly_type="implicit_fk",
        severity=AnomalySeverity.INFO,
        schema="public",
        table="orders",
        column="customer_id",
        description="Implicit relationship.",
        suggestion="Declare the foreign key.",
    )

    assert str(AnomalySeverity.WARNING) == "warning"
    assert anomaly.location == "public.orders.customer_id"
    assert anomaly.to_dict()["severity"] == "info"


def test_detect_no_indexes_and_no_pk_and_high_nullable() -> None:
    table = make_table(
        "orphan_rows",
        columns=[
            make_column("customer_id", "integer"),
            make_column("notes", "text"),
            make_column("value", "text"),
        ],
    )

    anomalies = AnomalyDetector().detect_table(table)
    anomaly_types = {item.anomaly_type for item in anomalies}

    assert {"no_indexes", "no_pk", "high_nullable_no_pk", "implicit_fk", "ambiguous_column_name"} <= anomaly_types


def test_detect_fk_without_index_and_ambiguous_name() -> None:
    table = make_table(
        "orders",
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("customer_id", "integer", nullable=False, foreign_key=True),
            make_column("valor", "numeric"),
        ],
        foreign_keys=[make_fk("orders", ["customer_id"], "customers")],
        indexes=[make_index("orders", ["id"], primary=True)],
    )

    anomalies = AnomalyDetector().detect_table(table)

    assert any(item.anomaly_type == "fk_without_index" for item in anomalies)
    assert any(item.anomaly_type == "ambiguous_column_name" and item.column == "valor" for item in anomalies)


def test_detect_empty_table_skips_staging_tables() -> None:
    regular = make_table(
        "empty_archive",
        columns=[make_column("id", "integer", primary_key=True, nullable=False)],
        indexes=[make_index("empty_archive", ["id"], primary=True)],
    )
    staging = make_table(
        "tmp_archive",
        columns=[make_column("payload", "text")],
    )
    staging.heuristic_type = "staging"

    regular_anomalies = AnomalyDetector().detect_table(regular)
    staging_anomalies = AnomalyDetector().detect_table(staging)

    assert any(item.anomaly_type == "empty_table" for item in regular_anomalies)
    assert not any(item.anomaly_type == "empty_table" for item in staging_anomalies)


def test_detect_wide_table_and_ignore_views_for_index_pk_rules() -> None:
    wide = make_table(
        "analytics_export",
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            *[make_column(f"col_{index}", "text") for index in range(60)],
        ],
        indexes=[make_index("analytics_export", ["id"], primary=True)],
    )
    view = make_table(
        "order_summary",
        table_type=TableType.VIEW,
        columns=[make_column("total", "numeric")],
    )

    wide_anomalies = AnomalyDetector().detect_table(wide)
    view_anomalies = AnomalyDetector().detect_table(view)

    assert any(item.anomaly_type == "wide_table" for item in wide_anomalies)
    assert not any(item.anomaly_type in {"no_indexes", "no_pk"} for item in view_anomalies)


def test_detect_sorts_by_severity_then_location_and_summarizes() -> None:
    first = make_table("b_table", columns=[make_column("field1", "text")])
    second = make_table(
        "a_table",
        columns=[make_column("customer_id", "integer"), make_column("field2", "text")],
        foreign_keys=[make_fk("a_table", ["customer_id"], "customers")],
    )
    result = make_result([first, second])

    anomalies = AnomalyDetector().detect(result)
    summary = AnomalyDetector().summarize(anomalies)

    assert anomalies[0].severity >= anomalies[-1].severity
    assert "no_indexes" in summary
    assert summary["ambiguous_column_name"] >= 2
