"""Integration tests for Phase 12C executive reports."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas.export.report_executive import ExecutiveReportGenerator
from tests.integration.phase_12.helpers import (
    build_phase12_result,
    build_phase12_snapshot,
    write_sigil_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_12c]


def _run_report(
    run_command,
    repo_root: Path,
    python_executable: str,
    *args: str,
):
    return run_command([python_executable, "-m", "atlas", "report", *args], cwd=repo_root)


def test_executive_report_generates_complete_offline_html(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "offline_exec.db")
    html = ExecutiveReportGenerator(result).build_html()

    assert html.startswith("<!DOCTYPE html>")
    assert "Atlas Executive Report" in html
    assert "<style>" in html
    assert "cdn.jsdelivr.net" not in html
    assert "cdnjs.cloudflare.com" not in html


def test_executive_report_overview_reflects_database_engine_and_timestamp(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "overview_exec.db")
    html = ExecutiveReportGenerator(result).build_html()

    assert result.database in html
    assert result.engine in html
    assert "Extracted at" in html
    assert result.introspected_at in html


def test_executive_report_contains_schema_and_top_table_sections(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "sections_exec.db")
    html = ExecutiveReportGenerator(result).build_html()

    assert "<h2>Schemas</h2>" in html
    assert "<h2>Top Tables</h2>" in html
    assert "main.fact_orders" in html


def test_executive_report_derives_recommendations_from_anomalies(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "recommend_exec.db")
    html = ExecutiveReportGenerator(result).build_html()

    assert "<h2>Recommendations</h2>" in html
    assert (
        "Stabilize table identity" in html
        or "Improve relationship performance" in html
        or "Clarify data vocabulary" in html
    )


def test_executive_report_semantic_section_appears_for_snapshot_semantics(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "semantic_exec.db")
    snapshot = build_phase12_snapshot(result)
    html = ExecutiveReportGenerator(
        snapshot.result,
        scores=snapshot.scores,
        anomalies=snapshot.anomalies,
        semantics=snapshot.semantics,
    ).build_html()

    assert "<h2>Semantic Coverage</h2>" in html
    assert "Top Business Domains" in html
    assert "Customer orders" in html


def test_cli_report_style_executive_from_sigil(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    result = build_phase12_result(phase_tmp_dir / "cli_exec_sigil.db")
    sigil_path = write_sigil_fixture(phase_tmp_dir, result, name="report_input")
    output_path = phase_tmp_dir / "executive.html"

    command = _run_report(
        run_command,
        repo_root,
        python_executable,
        "--sigil",
        str(sigil_path),
        "--style",
        "executive",
        "--output",
        str(output_path),
    )

    assert command.returncode == 0, command.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "Atlas Executive Report" in content
    assert "Top Tables" in content


def test_cli_report_style_executive_from_atlas_reuses_snapshot_scores_and_anomalies(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    result = build_phase12_result(phase_tmp_dir / "cli_exec_atlas.db")
    snapshot = build_phase12_snapshot(result)
    snapshot.scores = [
        {
            "table": "config_settings",
            "schema": "main",
            "qualified_name": "main.config_settings",
            "score": 9.999,
            "breakdown": {"volume_score": 0.0},
            "rank": 1,
        }
    ]
    snapshot.anomalies = [
        {
            "anomaly_type": "custom_gap",
            "severity": "warning",
            "schema": "main",
            "table": "config_settings",
            "column": None,
            "location": "main.config_settings",
            "description": "Custom anomaly from snapshot payload.",
            "suggestion": "Review snapshot contract handling.",
        }
    ]
    atlas_path = snapshot.save(phase_tmp_dir / "exec_snapshot")
    output_path = phase_tmp_dir / "executive_snapshot.html"

    command = _run_report(
        run_command,
        repo_root,
        python_executable,
        "--atlas",
        str(atlas_path),
        "--style",
        "executive",
        "--output",
        str(output_path),
    )

    assert command.returncode == 0, command.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "9.999" in content
    assert "Custom anomaly from snapshot payload." in content
