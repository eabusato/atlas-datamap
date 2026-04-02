"""Phase 3C integration tests for the native-build workflow."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from atlas._c import build_lib

pytestmark = [pytest.mark.integration, pytest.mark.phase_3c]


def test_build_helper_make_fallback_produces_library(repo_root: Path) -> None:
    result = build_lib.build_sigilo_library(repo_root=repo_root, prefer_cmake=False)
    assert result.success, result.message
    assert Path(result.library_path).exists()


def test_check_sigilo_build_script_reports_success_after_build(
    repo_root: Path,
    python_executable: str,
) -> None:
    build_result = build_lib.build_sigilo_library(repo_root=repo_root, prefer_cmake=False)
    assert build_result.success, build_result.message
    result = subprocess.run(
        [python_executable, str(repo_root / "scripts" / "check_sigilo_build.py")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "C extension available" in result.stdout


def test_setup_build_ext_inplace_succeeds(repo_root: Path, python_executable: str) -> None:
    result = subprocess.run(
        [python_executable, "setup.py", "build_ext", "--inplace"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_root_make_build_c_target_succeeds(repo_root: Path) -> None:
    result = subprocess.run(
        ["make", "build-c"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_root_make_clean_c_removes_library(repo_root: Path) -> None:
    build_result = build_lib.build_sigilo_library(repo_root=repo_root, prefer_cmake=False)
    assert build_result.success, build_result.message
    result = subprocess.run(
        ["make", "clean-c"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert not build_lib.library_path(repo_root).exists()
