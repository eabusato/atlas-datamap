"""Integration tests for the Atlas onboarding flow."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner

from atlas.cli.onboard import onboard_cmd
from atlas.onboarding import AISetup, DatabaseSetup, OnboardingManifest, run_onboarding

pytestmark = [pytest.mark.integration, pytest.mark.phase_14c]


def _build_sqlite_fixture(path: Path) -> Path:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                customer_code TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE payments (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL REFERENCES customers(id),
                amount NUMERIC NOT NULL,
                payment_status TEXT NOT NULL
            );

            INSERT INTO customers (customer_code, created_at)
            VALUES ('CUST-001', '2026-01-01T00:00:00Z'),
                   ('CUST-002', '2026-01-02T00:00:00Z');

            INSERT INTO payments (customer_id, amount, payment_status)
            VALUES (1, 120.50, 'approved'),
                   (2, 80.00, 'pending');
            """
        )
        connection.commit()
    finally:
        connection.close()
    return path


def test_run_onboarding_creates_full_workspace(tmp_path: Path) -> None:
    db_path = _build_sqlite_fixture(tmp_path / "demo.db")
    workspace = tmp_path / "workspace"
    manifest = OnboardingManifest(
        project_name="Demo Run",
        workspace_dir=str(workspace),
        database=DatabaseSetup(
            engine="sqlite",
            sqlite_path=str(db_path),
            database=str(db_path),
        ),
        ai=AISetup(enabled=False),
    )

    events: list[str] = []
    outputs = run_onboarding(manifest, on_progress=events.append)

    assert outputs.scan_svg.exists()
    assert outputs.scan_sigil.exists()
    assert outputs.scan_meta.exists()
    assert outputs.scan_panel_html.exists()
    assert outputs.scan_snapshot.exists()
    assert outputs.standalone_html.exists()
    assert outputs.health_report.exists()
    assert outputs.executive_report.exists()
    assert outputs.dictionary_json.exists()
    assert outputs.tables_csv.exists()
    assert outputs.columns_csv.exists()
    assert outputs.dictionary_md.exists()
    assert outputs.history_snapshot.exists()
    assert outputs.diff_report is None
    assert outputs.manifest_path.exists()
    assert outputs.connection_reference is not None and outputs.connection_reference.exists()
    assert any("Starting structural scan" in event for event in events)
    assert any("Loading tables for schema main" in event for event in events)
    assert any("Generating health report" in event for event in events)
    assert any("Full pipeline completed" in event for event in events)


def test_run_onboarding_second_run_generates_diff(tmp_path: Path) -> None:
    db_path = _build_sqlite_fixture(tmp_path / "demo.db")
    workspace = tmp_path / "workspace"
    manifest = OnboardingManifest(
        project_name="Demo Run",
        workspace_dir=str(workspace),
        database=DatabaseSetup(
            engine="sqlite",
            sqlite_path=str(db_path),
            database=str(db_path),
        ),
        ai=AISetup(enabled=False),
    )

    first_outputs = run_onboarding(manifest)
    assert first_outputs.diff_report is None

    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "ALTER TABLE payments ADD COLUMN payment_reference TEXT DEFAULT 'REF-001'"
        )
        connection.commit()
    finally:
        connection.close()

    second_outputs = run_onboarding(manifest)
    assert second_outputs.diff_report is not None
    assert second_outputs.diff_report.exists()
    history_files = sorted((workspace / "generated" / "history").glob("*.atlas"))
    assert len(history_files) >= 2


def test_onboard_cli_interactive_sqlite_flow(tmp_path: Path) -> None:
    db_path = _build_sqlite_fixture(tmp_path / "demo.db")
    workspace = tmp_path / "wizard"
    runner = CliRunner()
    user_input = "\n".join(
        [
            str(workspace),
            "Wizard Run",
            "generated",
            "network",
            "circular",
            "y",
            "sqlite",
            str(db_path),
            "30",
            "50",
            "masked",
            "",
            "",
            "n",
            "y",
        ]
    )

    result = runner.invoke(onboard_cmd, input=user_input)
    assert result.exit_code == 0, result.output
    assert (workspace / "atlas.onboard.json").exists()
    assert (workspace / "generated" / "scans").exists()
    assert "[atlas onboard] scan connect" in result.output
    assert "[atlas onboard] report Generating health report." in result.output


def test_onboard_cli_resume_reuses_manifest(tmp_path: Path) -> None:
    db_path = _build_sqlite_fixture(tmp_path / "resume.db")
    workspace = tmp_path / "resume"
    manifest = OnboardingManifest(
        project_name="Resume Run",
        workspace_dir=str(workspace),
        database=DatabaseSetup(
            engine="sqlite",
            sqlite_path=str(db_path),
            database=str(db_path),
        ),
        ai=AISetup(enabled=False),
    )
    manifest.save()

    runner = CliRunner()
    result = runner.invoke(onboard_cmd, ["--resume", str(manifest.manifest_path)])
    assert result.exit_code == 0, result.output
    assert (workspace / "generated" / "reports").exists()
    assert "[atlas onboard] Reusing manifest" in result.output
    assert "[atlas onboard] done Full pipeline completed" in result.output
