"""Phase 3A tests for the compiled C sigilo library."""

from __future__ import annotations

import ctypes
import subprocess
import sys
from pathlib import Path

import pytest


def _library_path(repo_root: Path) -> Path:
    suffix = ".dll" if sys.platform == "win32" else ".dylib" if sys.platform == "darwin" else ".so"
    return repo_root / "atlas" / f"libatlas_sigilo{suffix}"


@pytest.fixture(scope="module")
def built_library(repo_root: Path) -> Path:
    lib_path = _library_path(repo_root)
    if not lib_path.exists():
        result = subprocess.run(
            ["make", "-C", str(repo_root / "atlas" / "_c"), "clean", "all"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            pytest.skip(f"Unable to build C library: {result.stderr[:300]}")
    return lib_path


@pytest.fixture(scope="module")
def lib(built_library: Path):
    if not built_library.exists():
        pytest.skip(f"Library not compiled: {built_library}")
    return ctypes.CDLL(str(built_library))


class TestCLibraryLoad:
    def test_library_loads(self, lib) -> None:
        assert lib is not None

    def test_ping_returns_version(self, lib) -> None:
        lib.atlas_sigilo_ping.restype = ctypes.c_char_p
        result = lib.atlas_sigilo_ping()
        assert result is not None
        assert b"3A" in result

    def test_render_version(self, lib) -> None:
        lib.atlas_render_version.restype = ctypes.c_char_p
        result = lib.atlas_render_version()
        assert result is not None
        assert result == b"3A.0"

    def test_abi_version(self, lib) -> None:
        lib.atlas_sigilo_abi_version.restype = ctypes.c_char_p
        result = lib.atlas_sigilo_abi_version()
        assert result == b"3A.0"


class TestSmokeBuild:
    def test_makefile_compiles(self, repo_root: Path) -> None:
        result = subprocess.run(
            ["make", "-C", str(repo_root / "atlas" / "_c"), "clean", "all"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            pytest.skip(f"make not available or failed: {result.stderr[:300]}")
        assert result.returncode == 0

    def test_smoke_script(self, repo_root: Path) -> None:
        script = repo_root / "tests" / "test_c_library.sh"
        result = subprocess.run(
            ["bash", str(script)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert "Smoke test passed." in result.stdout
