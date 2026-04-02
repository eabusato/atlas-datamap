"""Phase 5A integration tests for the real ``atlas scan`` command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.integration.phase_0.helpers import build_sqlite_fixture

pytestmark = [pytest.mark.integration, pytest.mark.phase_5a]


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


def _run_scan(
    run_command,
    repo_root: Path,
    python_executable: str,
    *args: str,
):
    return run_command([python_executable, "-m", "atlas", "scan", *args], cwd=repo_root)


def test_scan_generates_three_artifacts(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "scan.db"
    build_sqlite_fixture(db_path)
    output_dir = phase_tmp_dir / "out"

    result = _run_scan(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--output",
        str(output_dir),
    )

    assert result.returncode == 0, result.stderr
    assert (output_dir / "scan.db.svg").exists()
    assert (output_dir / "scan.db.sigil").exists()
    assert (output_dir / "scan.db_meta.json").exists()


def test_scan_meta_json_contains_real_schema(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "meta.db"
    build_sqlite_fixture(db_path)
    output_dir = phase_tmp_dir / "out"

    result = _run_scan(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--output",
        str(output_dir),
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads((output_dir / "meta.db_meta.json").read_text(encoding="utf-8"))
    assert payload["database"].endswith("meta.db")
    assert payload["total_tables"] == 2
    assert payload["schemas"][0]["tables"][1]["foreign_keys"][0]["target_table"] == "customers"


def test_scan_dry_run_does_not_write_files(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "dry.db"
    build_sqlite_fixture(db_path)
    output_dir = phase_tmp_dir / "out"

    result = _run_scan(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--output",
        str(output_dir),
        "--dry-run",
    )

    assert result.returncode == 0, result.stderr
    assert not output_dir.exists()


def test_scan_compact_style_uses_compact_canvas(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "compact.db"
    build_sqlite_fixture(db_path)
    output_dir = phase_tmp_dir / "out"

    result = _run_scan(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--output",
        str(output_dir),
        "--style",
        "compact",
    )

    assert result.returncode == 0, result.stderr
    svg = (output_dir / "compact.db.svg").read_text(encoding="utf-8")
    assert 'width="800"' in svg
    assert 'height="800"' in svg


def test_scan_requires_force_for_overwrite(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "overwrite.db"
    build_sqlite_fixture(db_path)
    output_dir = phase_tmp_dir / "out"

    first = _run_scan(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--output",
        str(output_dir),
    )
    second = _run_scan(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--output",
        str(output_dir),
    )
    forced = _run_scan(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--output",
        str(output_dir),
        "--force",
    )

    assert first.returncode == 0, first.stderr
    assert second.returncode != 0
    assert "--force" in second.stderr
    assert forced.returncode == 0, forced.stderr


def test_scan_fails_when_schema_filter_removes_everything(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "filter.db"
    build_sqlite_fixture(db_path)
    output_dir = phase_tmp_dir / "out"

    result = _run_scan(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--output",
        str(output_dir),
        "--schema",
        "missing",
    )

    assert result.returncode != 0
    assert "renderable tables" in result.stderr
