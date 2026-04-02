"""Phase 14C integration coverage for local release packaging."""

from __future__ import annotations

import importlib.util
import subprocess
import tarfile
import zipfile
from pathlib import Path

import pytest

from atlas.version import ATLAS_VERSION

pytestmark = [pytest.mark.integration, pytest.mark.phase_14c]
PDOC_AVAILABLE = importlib.util.find_spec("pdoc") is not None


@pytest.fixture(scope="module")
def built_dist(tmp_path_factory: pytest.TempPathFactory, python_executable: str, repo_root: Path) -> dict[str, Path]:
    target = tmp_path_factory.mktemp("phase14c_build")
    dist_dir = target / "dist"
    subprocess.run(
        [python_executable, "-m", "build", "--no-isolation", "--outdir", str(dist_dir)],
        cwd=repo_root,
        check=True,
        text=True,
        capture_output=True,
    )
    wheel_path = next(dist_dir.glob("atlas_datamap-*.whl"))
    sdist_path = next(dist_dir.glob("atlas_datamap-*.tar.gz"))
    return {"wheel": wheel_path, "sdist": sdist_path, "root": target}


def test_distribution_build_produces_wheel_and_sdist(built_dist: dict[str, Path]) -> None:
    assert built_dist["wheel"].exists()
    assert built_dist["sdist"].exists()
    assert ATLAS_VERSION in built_dist["wheel"].name
    assert ATLAS_VERSION in built_dist["sdist"].name


def test_built_wheel_contains_public_sdk_and_type_marker(built_dist: dict[str, Path]) -> None:
    with zipfile.ZipFile(built_dist["wheel"]) as archive:
        members = set(archive.namelist())

    assert "atlas/sdk.py" in members
    assert "atlas/py.typed" in members
    assert any(name.endswith("METADATA") for name in members)


def test_built_wheel_contains_native_sigilo_or_packaged_fallback_assets(
    built_dist: dict[str, Path],
) -> None:
    with zipfile.ZipFile(built_dist["wheel"]) as archive:
        members = set(archive.namelist())

    assert "atlas/_sigilo.py" in members
    assert "atlas/_sigilo_build.py" in members
    assert any("libatlas_sigilo" in name for name in members)


def test_installed_wheel_imports_and_reports_version(
    built_dist: dict[str, Path],
    phase_tmp_dir: Path,
    python_executable: str,
) -> None:
    install_dir = phase_tmp_dir / "wheel_site"
    subprocess.run(
        [
            python_executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(install_dir),
            str(built_dist["wheel"]),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    result = subprocess.run(
        [
            python_executable,
            "-c",
            "import atlas; import atlas._sigilo as s; print(atlas.__version__); print(s.available()); print(s.load_error() or '')",
        ],
        env={"PYTHONPATH": str(install_dir)},
        check=True,
        text=True,
        capture_output=True,
    )

    lines = result.stdout.strip().splitlines()
    assert lines[0] == ATLAS_VERSION
    assert lines[1] == "True"


def test_installed_sdist_builds_and_loads_native_library(
    built_dist: dict[str, Path],
    phase_tmp_dir: Path,
    python_executable: str,
) -> None:
    install_dir = phase_tmp_dir / "sdist_site"
    subprocess.run(
        [
            python_executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(install_dir),
            str(built_dist["sdist"]),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    result = subprocess.run(
        [
            python_executable,
            "-c",
            "import atlas; import atlas._sigilo as s; print(atlas.__version__); print(s.available()); print(s.load_error() or '')",
        ],
        env={"PYTHONPATH": str(install_dir)},
        check=True,
        text=True,
        capture_output=True,
    )

    lines = result.stdout.strip().splitlines()
    assert lines[0] == ATLAS_VERSION
    assert lines[1] == "True"


def test_installed_wheel_runs_cli_help(
    built_dist: dict[str, Path],
    phase_tmp_dir: Path,
    python_executable: str,
) -> None:
    install_dir = phase_tmp_dir / "wheel_cli_site"
    subprocess.run(
        [
            python_executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(install_dir),
            str(built_dist["wheel"]),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    result = subprocess.run(
        [python_executable, "-m", "atlas", "--help"],
        env={"PYTHONPATH": str(install_dir)},
        check=True,
        text=True,
        capture_output=True,
    )

    assert "Usage:" in result.stdout
    assert "scan" in result.stdout


def test_sdist_includes_changelog_and_publishing_docs(built_dist: dict[str, Path]) -> None:
    with tarfile.open(built_dist["sdist"], "r:gz") as archive:
        members = set(archive.getnames())

    root_prefix = f"atlas_datamap-{ATLAS_VERSION}"
    assert f"{root_prefix}/CHANGELOG.md" in members
    assert f"{root_prefix}/docs/publishing.md" in members
    assert f"{root_prefix}/MANIFEST.in" in members


@pytest.mark.skipif(not PDOC_AVAILABLE, reason="pdoc is not installed")
def test_make_docs_generates_api_html(
    phase_tmp_dir: Path,
    python_executable: str,
    repo_root: Path,
) -> None:
    docs_dir = phase_tmp_dir / "api_docs"
    result = subprocess.run(
        ["make", f"PYTHON={python_executable}", f"DOCS_DIR={docs_dir}", "docs"],
        cwd=repo_root,
        check=True,
        text=True,
        capture_output=True,
    )

    assert "[atlas] Generating API documentation with pdoc..." in result.stdout
    assert (docs_dir / "atlas.html").exists()
