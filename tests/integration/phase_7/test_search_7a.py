"""Phase 7A integration tests for ``atlas search``."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.phase_7.helpers import build_phase7_sqlite_fixture

pytestmark = [pytest.mark.integration, pytest.mark.phase_7a]


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


def _run_search(
    run_command,
    repo_root: Path,
    python_executable: str,
    *args: str,
):
    return run_command([python_executable, "-m", "atlas", "search", *args], cwd=repo_root)


def test_search_finds_table_by_exact_name_tokens(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "search_exact.db"
    build_phase7_sqlite_fixture(db_path)

    result = _run_search(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "fact orders",
    )

    assert result.returncode == 0, result.stderr
    assert "[table] main.fact_orders" in result.stdout
    assert "L0 exact name token-set" in result.stdout


def test_search_can_filter_results_by_schema(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "search_schema.db"
    build_phase7_sqlite_fixture(db_path)

    result = _run_search(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--schema",
        "main",
        "customer",
    )

    assert result.returncode == 0, result.stderr
    assert "[table] main.customer_accounts" in result.stdout


def test_search_columns_finds_column_hits(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "search_columns.db"
    build_phase7_sqlite_fixture(db_path)

    result = _run_search(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--columns",
        "payment_status",
    )

    assert result.returncode == 0, result.stderr
    assert "[column] main.fact_orders.payment_status" in result.stdout


def test_search_type_filter_returns_only_matching_heuristic_tables(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "search_type.db"
    build_phase7_sqlite_fixture(db_path)

    result = _run_search(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "--type",
        "dimension",
        "customer",
    )

    assert result.returncode == 0, result.stderr
    assert "[table] main.customer_accounts" in result.stdout
    assert "main.fact_orders" not in result.stdout


def test_search_mixed_mode_can_return_schema_hits(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "search_schema_hit.db"
    build_phase7_sqlite_fixture(db_path)

    result = _run_search(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "main",
    )

    assert result.returncode == 0, result.stderr
    assert "[schema] main" in result.stdout


def test_search_returns_clear_message_when_no_match_exists(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    db_path = phase_tmp_dir / "search_none.db"
    build_phase7_sqlite_fixture(db_path)

    result = _run_search(
        run_command,
        repo_root,
        python_executable,
        "--db",
        _sqlite_url(db_path),
        "warehouse satellite",
    )

    assert result.returncode == 0, result.stderr
    assert "No matches found" in result.stdout
