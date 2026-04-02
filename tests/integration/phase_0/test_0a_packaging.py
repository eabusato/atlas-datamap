"""Integration coverage for Phase 0A packaging and CLI behavior."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from atlas.version import ATLAS_VERSION

pytestmark = [pytest.mark.integration, pytest.mark.phase_0a]


def _atlas_executable() -> str:
    bin_name = "atlas.exe" if os.name == "nt" else "atlas"
    return str(Path(sys.executable).parent / bin_name)


def test_console_help_works(run_command, repo_root) -> None:
    atlas_exe = _atlas_executable()
    result = run_command([atlas_exe, "--help"], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    assert "Usage:" in result.stdout
    assert "scan" in result.stdout


def test_python_module_help_works(run_command, repo_root, python_executable: str) -> None:
    result = run_command([python_executable, "-m", "atlas", "--help"], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    assert "Usage:" in result.stdout
    assert "scan" in result.stdout


def test_console_version_matches_package(run_command, repo_root) -> None:
    atlas_exe = _atlas_executable()
    result = run_command([atlas_exe, "--version"], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    assert ATLAS_VERSION in result.stdout
    assert "native sigilo" in result.stdout


def test_scan_help_lists_expected_options(run_command, repo_root) -> None:
    atlas_exe = _atlas_executable()
    result = run_command([atlas_exe, "scan", "--help"], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    assert "--db" in result.stdout
    assert "--config" in result.stdout
    assert "--output" in result.stdout


def test_make_help_lists_canonical_targets(run_command, repo_root) -> None:
    result = run_command(["make", "help"], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    assert "docs" in result.stdout
    assert "install-dev" in result.stdout
    assert "test-integration" in result.stdout
    assert "lint" in result.stdout


def test_build_generates_sdist_and_wheel(
    run_command,
    repo_root,
    phase_tmp_dir: Path,
    python_executable: str,
) -> None:
    outdir = phase_tmp_dir / "dist"
    result = run_command(
        [python_executable, "-m", "build", "--no-isolation", "--outdir", str(outdir)],
        cwd=repo_root,
    )
    assert result.returncode == 0, result.stderr
    artifacts = {path.name for path in outdir.iterdir()}
    assert any(name.endswith(".whl") for name in artifacts)
    assert any(name.endswith(".tar.gz") for name in artifacts)


def test_ask_help_lists_natural_language_options(
    run_command,
    repo_root,
) -> None:
    atlas_exe = _atlas_executable()
    result = run_command([atlas_exe, "ask", "--help"], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    assert "--sigil" in result.stdout
    assert "--interactive" in result.stdout
    assert "--no-embeddings" in result.stdout


def test_enrich_help_lists_semantic_options(run_command, repo_root) -> None:
    atlas_exe = _atlas_executable()
    result = run_command([atlas_exe, "enrich", "--help"], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    assert "--sigil" in result.stdout
    assert "--parallel" in result.stdout
    assert "--tables-only" in result.stdout


def test_diff_help_lists_snapshot_options(run_command, repo_root) -> None:
    atlas_exe = _atlas_executable()
    result = run_command([atlas_exe, "diff", "--help"], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    assert "--output" in result.stdout


def test_history_help_lists_subcommands(run_command, repo_root) -> None:
    atlas_exe = _atlas_executable()
    result = run_command([atlas_exe, "history", "--help"], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    assert "list" in result.stdout
    assert "diff" in result.stdout
    assert "open" in result.stdout
