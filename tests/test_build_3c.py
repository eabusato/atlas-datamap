"""Phase 3C tests for packaging and native-build integration metadata."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from atlas._c import build_lib

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def pyproject_data() -> dict[str, object]:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        return tomllib.load(handle)


def test_pyproject_declares_sigilo_optional_group(pyproject_data: dict[str, object]) -> None:
    project = pyproject_data["project"]  # type: ignore[index]
    optional = project["optional-dependencies"]  # type: ignore[index]
    assert "sigilo" in optional


def test_pyproject_package_data_includes_native_artifacts(
    pyproject_data: dict[str, object],
) -> None:
    tool = pyproject_data["tool"]  # type: ignore[index]
    package_data = tool["setuptools"]["package-data"]["atlas"]  # type: ignore[index]
    assert any("libatlas_sigilo" in entry for entry in package_data)
    assert any("_c/common" in entry for entry in package_data)


def test_setup_declares_custom_build_ext() -> None:
    content = (ROOT / "setup.py").read_text(encoding="utf-8")
    assert "CMakeBuildExt" in content
    assert "atlas/_c/build_lib.py" in content


def test_build_helper_computes_expected_library_name() -> None:
    expected_suffix = ".dll" if sys.platform == "win32" else ".dylib" if sys.platform == "darwin" else ".so"
    assert build_lib.library_filename().endswith(expected_suffix)


def test_makefile_exposes_sigilo_targets() -> None:
    content = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "build-c:" in content
    assert "test-sigilo:" in content
    assert "clean-c:" in content


def test_public_headers_define_cross_platform_export_macro() -> None:
    render_header = (ROOT / "atlas" / "_c" / "atlas_render.h").read_text(encoding="utf-8")
    sigilo_header = (ROOT / "atlas" / "_c" / "atlas_sigilo.h").read_text(encoding="utf-8")
    assert "ATLAS_SIGILO_API" in render_header
    assert "__declspec(dllexport)" in render_header
    assert "__declspec(dllimport)" in render_header
    assert "ATLAS_SIGILO_API const char *atlas_sigilo_ping" in sigilo_header
