"""Version and package bootstrap tests for Phase 0A."""

from __future__ import annotations

import sys

import pytest

import atlas
import atlas.cli as atlas_cli
from atlas.version import ATLAS_MIN_PYTHON, ATLAS_VERSION


def test_version_string_uses_semver_triplet() -> None:
    parts = ATLAS_VERSION.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)


def test_running_python_satisfies_minimum_version() -> None:
    assert sys.version_info >= ATLAS_MIN_PYTHON


def test_package_import_exposes_version() -> None:
    assert atlas.__version__ == ATLAS_VERSION


def test_cli_import_is_available() -> None:
    assert atlas_cli.cli is not None


def test_version_message_mentions_native_sigilo_or_fallback() -> None:
    message = atlas_cli._version_message()

    assert ATLAS_VERSION in message
    assert "native sigilo" in message


def test_public_exports_exist() -> None:
    for export_name in atlas.__all__:
        assert hasattr(atlas, export_name)


def test_cli_rejects_unsupported_python(monkeypatch, capsys) -> None:
    class FakeVersionInfo(tuple):
        major = 3
        minor = 10

    monkeypatch.setattr(atlas_cli.sys, "version_info", FakeVersionInfo((3, 10, 0)))
    with pytest.raises(SystemExit) as exc_info:
        atlas_cli._check_python_version()
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Python 3.11+ is required" in captured.err
