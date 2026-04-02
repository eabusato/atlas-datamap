"""Phase 7C integration tests for the real ``atlas report`` command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.integration.phase_7.helpers import (
    build_phase7_sqlite_fixture,
    introspect_phase7_sqlite,
    sqlite_url,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_7c]


def _run_report(
    run_command,
    repo_root: Path,
    python_executable: str,
    *args: str,
):
    return run_command([python_executable, "-m", "atlas", "report", *args], cwd=repo_root)


def test_report_generates_html_from_live_database(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "live.db"
    output_path = phase_tmp_dir / "report.html"
    build_phase7_sqlite_fixture(db_path)

    result = _run_report(
        run_command,
        repo_root,
        python_executable,
        "--db",
        sqlite_url(db_path),
        "--output",
        str(output_path),
    )

    assert result.returncode == 0, result.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "Atlas Health Report" in content
    assert "Structural Summary" in content
    assert "Architecture Map (Sigilo)" in content


def test_report_can_render_without_sigilo(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "nosigilo.db"
    output_path = phase_tmp_dir / "report.html"
    build_phase7_sqlite_fixture(db_path)

    result = _run_report(
        run_command,
        repo_root,
        python_executable,
        "--db",
        sqlite_url(db_path),
        "--output",
        str(output_path),
        "--no-sigilo",
    )

    assert result.returncode == 0, result.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "Sigilo unavailable" in content
    assert "<svg" not in content


def test_report_reads_prebuilt_sigil_snapshot(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "snapshot.db"
    sigil_path = phase_tmp_dir / "snapshot.sigil"
    output_path = phase_tmp_dir / "snapshot.html"
    build_phase7_sqlite_fixture(db_path)
    result = introspect_phase7_sqlite(db_path)
    sigil_path.write_text(json.dumps(result.to_dict()), encoding="utf-8")

    command = _run_report(
        run_command,
        repo_root,
        python_executable,
        "--sigil",
        str(sigil_path),
        "--output",
        str(output_path),
        "--no-sigilo",
    )

    assert command.returncode == 0, command.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "main.fact_orders" in content
    assert "main.customer_accounts" in content


def test_report_contains_rankings_and_anomalies(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "rankings.db"
    output_path = phase_tmp_dir / "rankings.html"
    build_phase7_sqlite_fixture(db_path)

    result = _run_report(
        run_command,
        repo_root,
        python_executable,
        "--db",
        sqlite_url(db_path),
        "--output",
        str(output_path),
        "--no-sigilo",
    )

    assert result.returncode == 0, result.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "Top 10 by Volume" in content
    assert "Top 10 by Connectivity" in content
    assert "Structural Anomalies" in content
    assert "fact_orders" in content


def test_report_contains_schema_map_breakdown(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "schema_map.db"
    output_path = phase_tmp_dir / "schema_map.html"
    build_phase7_sqlite_fixture(db_path)

    result = _run_report(
        run_command,
        repo_root,
        python_executable,
        "--db",
        sqlite_url(db_path),
        "--output",
        str(output_path),
        "--no-sigilo",
    )

    assert result.returncode == 0, result.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "Schema Map" in content
    assert "Est. Rows" in content
    assert "<code>main</code>" in content


def test_report_is_standalone_without_external_cdns(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "standalone.db"
    output_path = phase_tmp_dir / "standalone.html"
    build_phase7_sqlite_fixture(db_path)

    result = _run_report(
        run_command,
        repo_root,
        python_executable,
        "--db",
        sqlite_url(db_path),
        "--output",
        str(output_path),
        "--no-sigilo",
    )

    assert result.returncode == 0, result.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "<style>" in content
    assert "<script>" in content
    assert "cdn.jsdelivr.net" not in content
    assert "cdnjs.cloudflare.com" not in content


def test_report_requires_exactly_one_input_source(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    sigil_path = phase_tmp_dir / "fixture.sigil"
    sigil_path.write_text("{}", encoding="utf-8")

    result = _run_report(
        run_command,
        repo_root,
        python_executable,
        "--db",
        "sqlite:///ignored.db",
        "--sigil",
        str(sigil_path),
    )

    assert result.returncode != 0
    assert "Provide either --db/--config or --sigil" in result.stderr
