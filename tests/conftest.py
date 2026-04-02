"""Shared pytest fixtures that keep test artifacts inside the repository."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TESTS_TMP_ROOT = REPO_ROOT / "tests" / "tmp"


def _sanitize_node_id(node_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", node_id)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def tests_tmp_root() -> Path:
    TESTS_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    return TESTS_TMP_ROOT


@pytest.fixture()
def phase_tmp_dir(tests_tmp_root: Path, request: pytest.FixtureRequest) -> Path:
    target = tests_tmp_root / _sanitize_node_id(request.node.nodeid)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    return target


@pytest.fixture()
def run_command(repo_root: Path):
    def _run(
        args: list[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        check: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        merged_env = dict(os.environ)
        merged_env["PYTHONPATH"] = str(repo_root)
        if env is not None:
            merged_env.update(env)
            if "PYTHONPATH" in env:
                merged_env["PYTHONPATH"] = f"{repo_root}:{env['PYTHONPATH']}"
        result = subprocess.run(
            args,
            cwd=cwd or repo_root,
            env=merged_env,
            text=True,
            capture_output=True,
            check=check,
        )
        return result

    return _run


@pytest.fixture(scope="session")
def python_executable() -> str:
    return sys.executable
