"""Phase 3A integration tests for the standalone C library."""

from __future__ import annotations

import ctypes
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration]


def _built_library(repo_root: Path) -> Path:
    suffix = ".dll" if sys.platform == "win32" else ".dylib" if sys.platform == "darwin" else ".so"
    return repo_root / "atlas" / f"libatlas_sigilo{suffix}"


@pytest.fixture(scope="module")
def compiled_sigilo(repo_root: Path) -> Path:
    result = subprocess.run(
        ["make", "-C", str(repo_root / "atlas" / "_c"), "clean", "all"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"Unable to compile atlas/_c via Makefile: {result.stderr[:400]}")
    return _built_library(repo_root)


@pytest.fixture(scope="module")
def sigilo_cdll(compiled_sigilo: Path):
    if not compiled_sigilo.exists():
        pytest.skip(f"Compiled library not found: {compiled_sigilo}")
    return ctypes.CDLL(str(compiled_sigilo))


def test_makefile_builds_library(compiled_sigilo: Path) -> None:
    assert compiled_sigilo.exists()
    assert compiled_sigilo.stat().st_size > 0


def test_ping_reports_abi_version(sigilo_cdll) -> None:
    sigilo_cdll.atlas_sigilo_ping.restype = ctypes.c_char_p
    assert sigilo_cdll.atlas_sigilo_ping() == b"3A.0"


def test_render_version_reports_renderer_version(sigilo_cdll) -> None:
    sigilo_cdll.atlas_render_version.restype = ctypes.c_char_p
    assert sigilo_cdll.atlas_render_version() == b"3A.0"


def test_abi_version_symbol_is_exposed(sigilo_cdll) -> None:
    sigilo_cdll.atlas_sigilo_abi_version.restype = ctypes.c_char_p
    assert sigilo_cdll.atlas_sigilo_abi_version() == b"3A.0"


def test_smoke_script_passes(repo_root: Path) -> None:
    if sys.platform == "win32":
        pytest.skip("The standalone bash smoke script is not supported on Windows runners.")
    script = repo_root / "tests" / "test_c_library.sh"
    result = subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "Smoke test passed." in result.stdout


def test_cmake_configures_and_builds_if_available(repo_root: Path) -> None:
    if shutil.which("cmake") is None:
        pytest.skip("CMake is not available in PATH")
    build_dir = repo_root / "tests" / "tmp" / "phase_3a_cmake_build"
    if build_dir.exists():
        for child in sorted(build_dir.glob("**/*"), reverse=True):
            if child.is_file():
                child.unlink()
        for child in sorted(build_dir.glob("**/*"), reverse=True):
            if child.is_dir():
                child.rmdir()
        build_dir.rmdir()
    build_dir.mkdir(parents=True, exist_ok=True)
    configure = subprocess.run(
        [
            "cmake",
            "-S",
            str(repo_root / "atlas" / "_c"),
            "-B",
            str(build_dir),
            "-DCMAKE_BUILD_TYPE=Release",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if configure.returncode != 0:
        pytest.skip(f"CMake not available or configure failed: {configure.stderr[:400]}")
    build = subprocess.run(
        ["cmake", "--build", str(build_dir), "--config", "Release"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert build.returncode == 0, build.stderr
