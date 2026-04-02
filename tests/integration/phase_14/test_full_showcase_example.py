"""Smoke test for the end-to-end Atlas showcase example."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.phase_14a]


def test_full_showcase_script_generates_complete_bundle(
    phase_tmp_dir: Path,
    run_command,
    python_executable: str,
) -> None:
    output_root = phase_tmp_dir / "showcase"

    result = run_command(
        [
            python_executable,
            "examples/full_showcase/build_full_showcase.py",
            "--output-dir",
            str(output_root),
        ],
        check=True,
    )

    manifest = json.loads(result.stdout)
    generated_dir = output_root / "generated"
    assert manifest["database"] == "aurora_demo_v1"
    assert manifest["counts"]["tables"] >= 30
    assert manifest["counts"]["columns"] >= 200

    expected_files = [
        generated_dir / "showcase_manifest.json",
        generated_dir / "atlas.toml",
        generated_dir / "atlas.ai.ollama.toml",
        generated_dir / "scans" / "aurora_demo_v1.svg",
        generated_dir / "scans" / "aurora_demo_v1.sigil",
        generated_dir / "scans" / "aurora_demo_v1.atlas",
        generated_dir / "scans" / "aurora_demo_v1_panel.html",
        generated_dir / "reports" / "aurora_health_report.html",
        generated_dir / "reports" / "aurora_executive_report.html",
        generated_dir / "exports" / "dictionary.json",
        generated_dir / "exports" / "tables.csv",
        generated_dir / "exports" / "columns.csv",
        generated_dir / "exports" / "dictionary.md",
        generated_dir / "queries" / "search_payment_dispute.txt",
        generated_dir / "queries" / "discovery_risk_alerts.json",
        generated_dir / "queries" / "info_payments.json",
        generated_dir / "diff" / "aurora_demo_v2.atlas",
        generated_dir / "diff" / "aurora_demo_diff.html",
        generated_dir / "history" / "history_list.json",
    ]
    for path in expected_files:
        assert path.exists(), f"missing showcase artifact: {path}"

    history_list = json.loads((generated_dir / "history" / "history_list.json").read_text())
    assert len(history_list) == 2
    assert any(item["database"] == "aurora_demo_v2" for item in history_list)
    assert manifest["semantic_outputs"] == {}


def test_full_showcase_ollama_mode_fails_clearly_when_ollama_is_unavailable(
    phase_tmp_dir: Path,
    run_command,
    python_executable: str,
) -> None:
    output_root = phase_tmp_dir / "showcase_ollama"

    result = run_command(
        [
            python_executable,
            "examples/full_showcase/build_full_showcase.py",
            "--output-dir",
            str(output_root),
            "--enable-ollama",
        ],
        check=False,
    )

    assert result.returncode != 0
    assert "Ollama" in result.stderr or "Ollama" in result.stdout
