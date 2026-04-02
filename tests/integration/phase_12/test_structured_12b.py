"""Integration tests for Phase 12B structured exports."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas.cli.export import load_export_source
from atlas.export.structured import StructuredExporter
from tests.integration.phase_12.helpers import (
    build_phase12_result,
    write_sigil_fixture,
    write_snapshot_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_12b]


def _run_export(
    run_command,
    repo_root: Path,
    python_executable: str,
    *args: str,
):
    return run_command([python_executable, "-m", "atlas", "export", *args], cwd=repo_root)


def test_export_json_injects_table_semantic_data(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "json_tables.db")
    atlas_path = write_snapshot_fixture(phase_tmp_dir, result, name="semantic_tables")
    source = load_export_source(sigil_path=None, atlas_path=atlas_path)

    payload = json.loads(
        StructuredExporter(source.result, semantics=source.semantics).export_json()
    )
    fact_orders = next(
        table
        for schema in payload["schemas"]
        for table in schema["tables"]
        if table["name"] == "fact_orders"
    )

    assert fact_orders["semantic_data"]["semantic_short"] == "Customer orders"
    assert fact_orders["semantic_data"]["semantic_domain"] == "sales"


def test_export_json_injects_column_semantic_data(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "json_columns.db")
    atlas_path = write_snapshot_fixture(phase_tmp_dir, result, name="semantic_columns")
    source = load_export_source(sigil_path=None, atlas_path=atlas_path)

    payload = json.loads(
        StructuredExporter(source.result, semantics=source.semantics).export_json()
    )
    payment_status = next(
        column
        for schema in payload["schemas"]
        for table in schema["tables"]
        if table["name"] == "fact_orders"
        for column in table["columns"]
        if column["name"] == "payment_status"
    )

    assert payment_status["semantic_data"]["semantic_short"] == "Payment status"
    assert payment_status["semantic_data"]["semantic_role"] == "payment_status"


def test_export_csv_tables_has_stable_headers_and_utf8(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "csv_tables.db")
    table = result.get_table("main", "config_settings")
    assert table is not None
    table.comment = "Café settings"

    csv_payload = StructuredExporter(result).export_csv_tables()

    assert csv_payload.startswith(
        "Schema,Table,Physical Type,Estimated Rows,Size Bytes,Comment,Semantic Summary,Semantic Domain,Semantic Role,Semantic Confidence"
    )
    assert "Café settings" in csv_payload


def test_export_csv_columns_serializes_metrics_and_flags(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "csv_columns.db")
    orders = result.get_table("main", "fact_orders")
    assert orders is not None
    payment_status = next(column for column in orders.columns if column.name == "payment_status")
    payment_status.is_indexed = True
    payment_status.stats.distinct_count = 4
    payment_status.stats.row_count = 8
    payment_status.stats.null_count = 1

    csv_payload = StructuredExporter(result).export_csv_columns()

    assert "Schema,Table,Column,Native Type,Canonical Type,Nullable,Primary Key,Foreign Key,Indexed,Distinct Count,Null Rate,Comment,Semantic Summary,Semantic Role,Semantic Confidence" in csv_payload
    assert "fact_orders,payment_status" in csv_payload
    assert ",true,4,0.1250," in csv_payload


def test_export_markdown_generates_schema_and_table_dictionary(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "markdown.db")
    markdown = StructuredExporter(result).export_markdown()

    assert markdown.startswith(f"# Atlas Export: {result.database}")
    assert "## Schema `main`" in markdown
    assert "### Table `main.fact_orders`" in markdown
    assert "| Column | Native Type | Canonical Type |" in markdown


def test_cli_export_csv_tables_writes_file(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    result = build_phase12_result(phase_tmp_dir / "cli_csv.db")
    sigil_path = write_sigil_fixture(phase_tmp_dir, result, name="csv_input")
    output_path = phase_tmp_dir / "tables.csv"

    command = _run_export(
        run_command,
        repo_root,
        python_executable,
        "csv",
        "--sigil",
        str(sigil_path),
        "--entity",
        "tables",
        "--output",
        str(output_path),
    )

    assert command.returncode == 0, command.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "Schema,Table,Physical Type" in content
    assert "fact_orders" in content


def test_cli_export_markdown_from_atlas_uses_snapshot_semantics(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    result = build_phase12_result(phase_tmp_dir / "cli_markdown.db")
    atlas_path = write_snapshot_fixture(phase_tmp_dir, result, name="markdown_input")
    output_path = phase_tmp_dir / "dictionary.md"

    command = _run_export(
        run_command,
        repo_root,
        python_executable,
        "markdown",
        "--atlas",
        str(atlas_path),
        "--output",
        str(output_path),
    )

    assert command.returncode == 0, command.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "Semantic summary: Customer orders" in content
    assert "Semantic domain: `sales`" in content
