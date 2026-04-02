"""Phase 4A integration tests for datamap sigilo generation."""

from __future__ import annotations

import math
import re

import pytest

from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_4a]


def _result() -> IntrospectionResult:
    accounts = TableInfo(
        name="accounts",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=2_400,
        size_bytes=65_536,
        columns=[
            ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="email", native_type="varchar(255)", is_nullable=False),
        ],
        indexes=[
            IndexInfo(name="accounts_pkey", table="accounts", schema="public", columns=["id"], is_primary=True),
        ],
    )
    orders = TableInfo(
        name="orders",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=48_000,
        size_bytes=524_288,
        columns=[
            ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="account_id", native_type="bigint", is_nullable=False, is_foreign_key=True),
        ],
        foreign_keys=[
            ForeignKeyInfo(
                name="fk_orders_accounts",
                source_schema="public",
                source_table="orders",
                source_columns=["account_id"],
                target_schema="public",
                target_table="accounts",
                target_columns=["id"],
                on_delete="CASCADE",
            )
        ],
        indexes=[
            IndexInfo(name="orders_pkey", table="orders", schema="public", columns=["id"], is_primary=True),
            IndexInfo(name="idx_orders_account", table="orders", schema="public", columns=["account_id"]),
        ],
    )
    active_accounts = TableInfo(
        name="active_accounts",
        schema="analytics",
        table_type=TableType.VIEW,
        row_count_estimate=2_000,
        size_bytes=8_192,
        columns=[ColumnInfo(name="account_id", native_type="bigint", is_nullable=False)],
    )
    account_rollup = TableInfo(
        name="account_rollup",
        schema="analytics",
        table_type=TableType.MATERIALIZED_VIEW,
        row_count_estimate=365,
        size_bytes=16_384,
        columns=[ColumnInfo(name="day_bucket", native_type="date", is_nullable=False)],
    )
    return IntrospectionResult(
        database="atlas",
        engine="postgresql",
        host="localhost",
        schemas=[
            SchemaInfo(name="public", engine="postgresql", tables=[accounts, orders]),
            SchemaInfo(name="analytics", engine="postgresql", tables=[active_accounts, account_rollup]),
        ],
    )


def test_datamap_builds_svg(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert "<svg" in svg
    assert 'class="system-node-wrap"' in svg


def test_datamap_exposes_numeric_metadata_attributes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert re.search(r'data-row-estimate="-?\d+"', svg)
    assert re.search(r'data-size-bytes="-?\d+"', svg)


def test_datamap_marks_views_and_materialized_views(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert 'class="node-aux"' in svg
    assert 'class="node-loop"' in svg


def test_datamap_marks_primary_key_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert 'class="col-pk"' in svg


def test_datamap_contains_database_foundation_and_core_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert 'id="database_foundation"' in svg
    assert "atlas" in svg
    assert "postgresql @ localhost" in svg


def test_datamap_contains_table_internal_structure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert 'class="node-shell"' in svg
    assert 'class="col-orbit"' in svg
    assert 'class="col-spoke"' in svg
    assert 'data-columns-detail="' in svg


def test_datamap_compact_style_uses_expected_canvas(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).set_style("compact").build().decode("utf-8")

    width = re.search(r'width="(\d+)"', svg)
    height = re.search(r'height="(\d+)"', svg)
    assert width is not None and int(width.group(1)) >= 800
    assert height is not None and int(height.group(1)) >= 800


def test_datamap_schema_rings_do_not_overlap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")
    schemas = [
        (float(cx), float(cy), float(r))
        for cx, cy, r in re.findall(
            r'<g class="system-schema-wrap" data-schema="[^"]+".*?<circle class="macro-ring" cx="([0-9.]+)" cy="([0-9.]+)" r="([0-9.]+)"/>',
            svg,
            re.S,
        )
    ]

    assert len(schemas) == 2
    x1, y1, r1 = schemas[0]
    x2, y2, r2 = schemas[1]
    assert math.hypot(x1 - x2, y1 - y2) > (r1 + r2)


def test_datamap_scales_svg_fonts_up_by_fifteen_percent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert "font: bold 12.65px monospace;" in svg
    assert "font: 12.65px monospace;" in svg
    assert "font: 10.35px monospace;" in svg
