"""Phase 5C integration tests for the real ``atlas info`` command."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from tests.integration.phase_0.helpers import build_sqlite_fixture

pytestmark = [pytest.mark.integration, pytest.mark.phase_5c]


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


def _run_info(
    run_command,
    repo_root: Path,
    python_executable: str,
    *args: str,
):
    return run_command([python_executable, "-m", "atlas", "info", *args], cwd=repo_root)


def test_info_text_reports_columns_fks_and_indexes(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "info.db"
    build_sqlite_fixture(db_path)
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("CREATE INDEX idx_orders_customer ON orders(customer_id)")
        connection.commit()
    finally:
        connection.close()

    result = _run_info(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--table",
        "main.orders",
    )

    assert result.returncode == 0, result.stderr
    assert "main.orders" in result.stdout
    assert "COLUMNS (3)" in result.stdout
    assert "FOREIGN KEYS (1)" in result.stdout
    assert "INDEXES" in result.stdout


def test_info_json_is_valid(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "json.db"
    build_sqlite_fixture(db_path)

    result = _run_info(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--table",
        "main.orders",
        "--format",
        "json",
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["name"] == "orders"
    assert payload["schema"] == "main"


def test_info_yaml_uses_plain_text_output(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "yaml.db"
    build_sqlite_fixture(db_path)

    result = _run_info(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--table",
        "main.orders",
        "--format",
        "yaml",
    )

    assert result.returncode == 0, result.stderr
    assert "name: orders" in result.stdout
    assert "foreign_keys:" in result.stdout


def test_info_no_columns_omits_column_section(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "nocol.db"
    build_sqlite_fixture(db_path)

    result = _run_info(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--table",
        "main.orders",
        "--no-columns",
    )

    assert result.returncode == 0, result.stderr
    assert "COLUMNS (" not in result.stdout
    assert "FOREIGN KEYS (1)" in result.stdout


def test_info_defaults_table_without_schema_to_public_like_parser(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "default.db"
    build_sqlite_fixture(db_path)

    result = _run_info(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--table",
        "orders",
    )

    assert result.returncode == 0, result.stderr
    assert "public.orders" in result.stdout


def test_info_reports_missing_table(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "missing.db"
    build_sqlite_fixture(db_path)

    result = _run_info(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--table",
        "main.missing",
    )

    assert result.returncode != 0
    assert "main.missing" in result.stderr
