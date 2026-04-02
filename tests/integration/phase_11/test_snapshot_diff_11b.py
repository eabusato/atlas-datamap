"""Integration tests for Phase 11B snapshot diff flows."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from atlas.cli.diff import diff_cmd
from atlas.export.diff import SnapshotDiff
from atlas.export.diff_report import SnapshotDiffReport
from atlas.types import ColumnInfo, ForeignKeyInfo, IntrospectionResult, TableInfo
from tests.integration.phase_11.helpers import build_phase11_semantic_result, build_snapshot_fixture

pytestmark = [pytest.mark.integration, pytest.mark.phase_11b]


def _clone_result(result: IntrospectionResult) -> IntrospectionResult:
    return IntrospectionResult.from_dict(result.to_dict())


def _snapshot_path(base_dir: Path, name: str, result: IntrospectionResult) -> Path:
    snapshot = build_snapshot_fixture(result)
    return snapshot.save(base_dir / name)


def test_diff_detects_added_and_removed_tables(phase_tmp_dir: Path) -> None:
    base = build_phase11_semantic_result(phase_tmp_dir / "tables.db")
    before = _clone_result(base)
    after = _clone_result(base)

    before_schema = before.get_schema("main")
    assert before_schema is not None
    before_schema.tables.append(
        TableInfo(
            name="legacy_archive",
            schema="main",
            columns=[ColumnInfo(name="id", native_type="integer", is_primary_key=True)],
        )
    )
    before_schema.refresh_derived_fields()
    before._compute_summary()

    schema = after.get_schema("main")
    assert schema is not None
    schema.tables.append(
        TableInfo(
            name="audit_events",
            schema="main",
            columns=[ColumnInfo(name="id", native_type="integer", is_primary_key=True)],
        )
    )
    schema.tables = [table for table in schema.tables if table.name != "config_settings"]
    schema.refresh_derived_fields()
    after._compute_summary()

    diff = SnapshotDiff.compare(build_snapshot_fixture(before), build_snapshot_fixture(after))
    assert "main.audit_events" in diff.added_tables
    assert "main.legacy_archive" in diff.removed_tables
    assert "main.config_settings" in diff.removed_tables


def test_diff_detects_column_and_type_changes(phase_tmp_dir: Path) -> None:
    base = build_phase11_semantic_result(phase_tmp_dir / "columns.db")
    before = _clone_result(base)
    after = _clone_result(base)

    orders_before = before.get_table("main", "fact_orders")
    orders_after = after.get_table("main", "fact_orders")
    assert orders_before is not None
    assert orders_after is not None

    orders_after.columns = [column for column in orders_after.columns if column.name != "payment_status"]
    orders_after.columns.append(ColumnInfo(name="billing_region", native_type="text"))
    orders_after.refresh_derived_fields()

    orders_before.columns.append(ColumnInfo(name="legacy_flag", native_type="boolean"))
    orders_before.refresh_derived_fields()

    for column in orders_after.columns:
        if column.name == "total_amount":
            column.native_type = "decimal(18,2)"

    diff = SnapshotDiff.compare(build_snapshot_fixture(before), build_snapshot_fixture(after))
    assert "payment_status" in diff.removed_columns["main.fact_orders"]
    assert "billing_region" in diff.added_columns["main.fact_orders"]
    assert "legacy_flag" in diff.removed_columns["main.fact_orders"]
    assert any(
        change.table == "main.fact_orders"
        and change.column == "total_amount"
        and change.new_type == "decimal(18,2)"
        for change in diff.type_changes
    )


def test_diff_applies_volume_thresholds(phase_tmp_dir: Path) -> None:
    base = build_phase11_semantic_result(phase_tmp_dir / "volume.db")
    before = _clone_result(base)
    after = _clone_result(base)

    orders_before = before.get_table("main", "fact_orders")
    orders_after = after.get_table("main", "fact_orders")
    settings_before = before.get_table("main", "config_settings")
    settings_after = after.get_table("main", "config_settings")
    assert orders_before is not None and orders_after is not None
    assert settings_before is not None and settings_after is not None

    orders_before.row_count_estimate = 2_000
    orders_after.row_count_estimate = 3_000
    settings_before.row_count_estimate = 50
    settings_after.row_count_estimate = 500

    diff = SnapshotDiff.compare(build_snapshot_fixture(before), build_snapshot_fixture(after))
    assert any(change.table == "main.fact_orders" for change in diff.volume_changes)
    assert all(change.table != "main.config_settings" for change in diff.volume_changes)


def test_diff_detects_relation_changes(phase_tmp_dir: Path) -> None:
    base = build_phase11_semantic_result(phase_tmp_dir / "relations.db")
    before = _clone_result(base)
    after = _clone_result(base)

    order_items_before = before.get_table("main", "order_items")
    order_items_after = after.get_table("main", "order_items")
    assert order_items_before is not None and order_items_after is not None

    removed_fk = order_items_after.foreign_keys.pop()
    order_items_before.foreign_keys.append(
        ForeignKeyInfo(
            name="fk_order_items_fact_orders_legacy",
            source_schema="main",
            source_table="order_items",
            source_columns=["order_id"],
            target_schema="main",
            target_table="fact_orders",
            target_columns=["id"],
        )
    )
    order_items_after.foreign_keys.append(
        ForeignKeyInfo(
            name="fk_order_items_customer_accounts",
            source_schema="main",
            source_table="order_items",
            source_columns=["order_id"],
            target_schema="main",
            target_table="customer_accounts",
            target_columns=["id"],
        )
    )

    diff = SnapshotDiff.compare(build_snapshot_fixture(before), build_snapshot_fixture(after))
    assert any(fk.name == "fk_order_items_customer_accounts" for fk in diff.new_relations)
    assert any(fk.name == removed_fk.name for fk in diff.removed_relations)
    assert any(fk.name == "fk_order_items_fact_orders_legacy" for fk in diff.removed_relations)


def test_diff_report_contains_required_sections(phase_tmp_dir: Path) -> None:
    base = build_phase11_semantic_result(phase_tmp_dir / "report.db")
    before = _clone_result(base)
    after = _clone_result(base)
    before.get_table("main", "fact_orders").row_count_estimate = 2_000  # type: ignore[union-attr]
    after.get_table("main", "fact_orders").row_count_estimate = 3_000  # type: ignore[union-attr]
    after.get_schema("main").tables.append(  # type: ignore[union-attr]
        TableInfo(
            name="new_dimension",
            schema="main",
            columns=[ColumnInfo(name="id", native_type="integer", is_primary_key=True)],
        )
    )
    after._compute_summary()

    before_snapshot = build_snapshot_fixture(before)
    after_snapshot = build_snapshot_fixture(after)
    diff = SnapshotDiff.compare(before_snapshot, after_snapshot)
    html = SnapshotDiffReport().render(before_snapshot, after_snapshot, diff)

    assert "Atlas Snapshot Diff" in html
    assert "Comparative Sigilo" in html
    assert "Relations" in html
    assert "main.new_dimension" in html
    assert "data-atlas-zoom-in" in html
    assert "data-atlas-zoom-fit" in html
    assert html.count('data-atlas-zoom-sync="snapshot-diff"') == 2
    assert "is-dragging" in html
    assert "Rows Before" in html
    assert "Rows After" in html
    assert "Net Row Delta" in html
    assert "offline .atlas snapshots" in html


def test_cli_diff_writes_report(phase_tmp_dir: Path) -> None:
    runner = CliRunner()
    before = build_phase11_semantic_result(phase_tmp_dir / "cli_before.db")
    after = _clone_result(before)
    after.get_table("main", "fact_orders").row_count_estimate = 4_000  # type: ignore[union-attr]
    before.get_table("main", "fact_orders").row_count_estimate = 2_000  # type: ignore[union-attr]

    before_path = _snapshot_path(phase_tmp_dir, "before", before)
    after_path = _snapshot_path(phase_tmp_dir, "after", after)
    output_path = phase_tmp_dir / "diff.html"

    result = runner.invoke(diff_cmd, [str(before_path), str(after_path), "--output", str(output_path)])
    assert result.exit_code == 0, result.output
    assert output_path.exists()
    assert "Wrote report" in result.output
    assert "volume_changes=1" in result.output
