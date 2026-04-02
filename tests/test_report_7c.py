"""Phase 7C unit tests for HTML report generation and CLI wiring."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from atlas.analysis.anomalies import AnomalySeverity, StructuralAnomaly
from atlas.cli.report import report_cmd
from atlas.export.report import (
    HTMLReportGenerator,
    _anomaly_card_class,
    _human_bytes,
    _severity_badge,
)
from atlas.types import ColumnInfo, IntrospectionResult, SchemaInfo, TableInfo, TableType


def _result() -> IntrospectionResult:
    return IntrospectionResult(
        database="atlas_reporting",
        engine="sqlite",
        host="localhost",
        schemas=[
            SchemaInfo(
                name="main",
                engine="sqlite",
                tables=[
                    TableInfo(
                        name="customer_accounts",
                        schema="main",
                        table_type=TableType.TABLE,
                        row_count_estimate=8_500,
                        size_bytes=1_048_576,
                        comment="Customer account registry",
                        columns=[
                            ColumnInfo(
                                name="id",
                                native_type="integer",
                                is_primary_key=True,
                                is_nullable=False,
                            ),
                            ColumnInfo(
                                name="email_address",
                                native_type="text",
                                is_nullable=False,
                            ),
                        ],
                    ),
                    TableInfo(
                        name="fact_orders",
                        schema="main",
                        table_type=TableType.TABLE,
                        row_count_estimate=125_000,
                        size_bytes=8_388_608,
                        comment="Order fact table",
                        columns=[
                            ColumnInfo(
                                name="id",
                                native_type="integer",
                                is_primary_key=True,
                                is_nullable=False,
                            ),
                            ColumnInfo(
                                name="customer_id",
                                native_type="integer",
                                is_foreign_key=True,
                                is_nullable=False,
                            ),
                        ],
                    ),
                ],
            )
        ],
        fk_in_degree_map={"main.customer_accounts": ["main.fact_orders"]},
    )


def test_human_bytes_formats_units() -> None:
    assert _human_bytes(0) == "—"
    assert _human_bytes(512) == "512 B"
    assert _human_bytes(2_048).endswith("KB")


def test_severity_badge_uses_expected_classes() -> None:
    assert "badge-critical" in _severity_badge(AnomalySeverity.CRITICAL)
    assert "badge-warning" in _severity_badge(AnomalySeverity.WARNING)
    assert "badge-info" in _severity_badge(AnomalySeverity.INFO)


def test_anomaly_card_class_maps_by_severity() -> None:
    assert _anomaly_card_class(AnomalySeverity.CRITICAL).endswith("critical")
    assert _anomaly_card_class(AnomalySeverity.WARNING).endswith("warning")
    assert _anomaly_card_class(AnomalySeverity.INFO).endswith("info")


def test_report_generator_writes_standalone_html(tmp_path: Path) -> None:
    output = tmp_path / "report.html"

    html = HTMLReportGenerator(_result())._render_html(
        scores=[],
        anomalies=[],
        svg_content="<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>",
    )
    output.write_text(html, encoding="utf-8")

    content = output.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in content
    assert "<style>" in content
    assert "<script>" in content
    assert "Atlas Health Report" in content
    assert "Structural Summary" in content
    assert "data-atlas-zoom-fit" in content
    assert ".svg-container .atlas-zoom-shell{flex:1;min-height:536px}" in content
    assert ".svg-container .atlas-zoom-viewport{height:100%;min-height:470px}" in content


def test_report_generator_renders_anomalies_section(tmp_path: Path) -> None:
    output = tmp_path / "report.html"
    generator = HTMLReportGenerator(_result())
    anomalies = [
        StructuralAnomaly(
            anomaly_type="no_indexes",
            severity=AnomalySeverity.WARNING,
            schema="main",
            table="fact_orders",
            description="Table has no indexes declared.",
            suggestion="Create an index.",
        )
    ]

    html = generator._render_html(scores=[], anomalies=anomalies, svg_content=None)
    output.write_text(html, encoding="utf-8")

    content = output.read_text(encoding="utf-8")
    assert "Structural Anomalies" in content
    assert "no_indexes" in content
    assert "main.fact_orders" in content


def test_report_cli_loads_sigil_and_writes_output(tmp_path: Path) -> None:
    sigil_path = tmp_path / "fixture.sigil"
    output_path = tmp_path / "out.html"
    sigil_path.write_text(json.dumps(_result().to_dict()), encoding="utf-8")

    result = CliRunner().invoke(
        report_cmd,
        ["--sigil", str(sigil_path), "--output", str(output_path), "--no-sigilo"],
    )

    assert result.exit_code == 0, result.output
    assert output_path.exists()


def test_report_cli_rejects_missing_input() -> None:
    result = CliRunner().invoke(report_cmd, ["--output", "report.html"])

    assert result.exit_code != 0
    assert "Provide either --db/--config or --sigil" in result.output


def test_report_cli_rejects_mixed_live_and_sigil_sources(tmp_path: Path) -> None:
    sigil_path = tmp_path / "fixture.sigil"
    sigil_path.write_text(json.dumps(_result().to_dict()), encoding="utf-8")

    result = CliRunner().invoke(
        report_cmd,
        ["--db", "sqlite:///dummy.db", "--sigil", str(sigil_path)],
    )

    assert result.exit_code != 0
    assert "Provide either --db/--config or --sigil" in result.output
