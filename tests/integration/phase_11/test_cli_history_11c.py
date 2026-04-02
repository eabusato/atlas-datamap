"""Integration tests for Phase 11C history management."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from atlas.cli.history import history_group
from atlas.export.snapshot import AtlasSnapshot
from atlas.history import AtlasHistory
from tests.integration.phase_11.helpers import build_phase11_semantic_result, build_snapshot_fixture

pytestmark = [pytest.mark.integration, pytest.mark.phase_11c]


def _write_snapshot(directory: Path, *, created_at: str, database: str = "aurora_bank") -> Path:
    result = build_phase11_semantic_result(directory / f"{database}_{created_at[:10]}.db")
    result.database = database
    snapshot = build_snapshot_fixture(result)
    snapshot.manifest.created_at = created_at
    history = AtlasHistory(directory)
    return snapshot.save(directory / history.build_snapshot_name(database, created_at))


def test_history_list_empty_directory(phase_tmp_dir: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(history_group, ["list", "--dir", str(phase_tmp_dir)])
    assert result.exit_code == 0
    assert "No valid snapshots found" in result.output


def test_history_list_orders_snapshots_descending(phase_tmp_dir: Path) -> None:
    _write_snapshot(phase_tmp_dir, created_at="2026-03-20T09:10:00Z")
    _write_snapshot(phase_tmp_dir, created_at="2026-03-31T14:30:22Z")

    runner = CliRunner()
    result = runner.invoke(history_group, ["list", "--dir", str(phase_tmp_dir)])
    assert result.exit_code == 0
    lines = [line for line in result.output.splitlines() if "aurora_bank" in line]
    assert "2026-03-31 14:30:22" in lines[0]
    assert "2026-03-20 09:10:00" in lines[1]


def test_history_resolve_latest_returns_newest_snapshot(phase_tmp_dir: Path) -> None:
    older = _write_snapshot(phase_tmp_dir, created_at="2026-03-20T09:10:00Z")
    newer = _write_snapshot(phase_tmp_dir, created_at="2026-03-31T14:30:22Z")
    history = AtlasHistory(phase_tmp_dir)

    assert history.latest() == newer
    assert history.resolve_snapshot("latest") == newer
    assert older != newer


def test_history_diff_resolves_aliases_and_writes_report(phase_tmp_dir: Path) -> None:
    _write_snapshot(phase_tmp_dir, created_at="2026-03-20T09:10:00Z")
    _write_snapshot(phase_tmp_dir, created_at="2026-03-31T14:30:22Z")

    runner = CliRunner()
    output_path = phase_tmp_dir / "history_diff.html"
    result = runner.invoke(
        history_group,
        [
            "diff",
            "--dir",
            str(phase_tmp_dir),
            "--from",
            "20260320",
            "--to",
            "latest",
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert output_path.exists()
    assert "Wrote diff report" in result.output


def test_history_resolve_rejects_ambiguous_date_reference(phase_tmp_dir: Path) -> None:
    _write_snapshot(phase_tmp_dir, created_at="2026-03-31T09:10:00Z", database="bank_a")
    _write_snapshot(phase_tmp_dir, created_at="2026-03-31T11:10:00Z", database="bank_b")
    history = AtlasHistory(phase_tmp_dir)

    with pytest.raises(ValueError, match="ambiguous"):
        history.resolve_snapshot("20260331")


def test_history_open_latest_uses_resolved_snapshot(
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    older = _write_snapshot(phase_tmp_dir, created_at="2026-03-20T09:10:00Z", database="old_bank")
    newer = _write_snapshot(phase_tmp_dir, created_at="2026-03-31T14:30:22Z", database="aurora_bank")
    captured: dict[str, str] = {}

    def _fake_start(self: object) -> None:
        captured["html"] = self._html_bytes.decode("utf-8")  # type: ignore[attr-defined]

    monkeypatch.setattr("atlas.cli.history.AtlasLocalServer.start", _fake_start)

    runner = CliRunner()
    result = runner.invoke(
        history_group,
        ["open", "--dir", str(phase_tmp_dir), "--date", "latest"],
    )
    assert result.exit_code == 0, result.output
    assert newer.name in result.output
    assert older.name not in result.output
    assert "aurora_bank" in captured["html"]


def test_history_list_ignores_corrupted_archives(phase_tmp_dir: Path) -> None:
    _write_snapshot(phase_tmp_dir, created_at="2026-03-20T09:10:00Z")
    (phase_tmp_dir / "broken.atlas").write_text("not a zip", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(history_group, ["list", "--dir", str(phase_tmp_dir)])
    assert result.exit_code == 0
    assert "broken.atlas" not in result.output
    assert "aurora_bank" in result.output


def test_snapshot_load_roundtrip_used_by_history_helpers(phase_tmp_dir: Path) -> None:
    path = _write_snapshot(phase_tmp_dir, created_at="2026-03-20T09:10:00Z")
    loaded = AtlasSnapshot.load(path)
    assert loaded.manifest.database == "aurora_bank"
    assert loaded.result.database == "aurora_bank"
