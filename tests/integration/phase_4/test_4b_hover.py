"""Phase 4B integration tests for hover-enabled SVG output."""

from __future__ import annotations

import re

import pytest

from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_4b]


def _result() -> IntrospectionResult:
    accounts = TableInfo(
        name="accounts",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=1_250,
        size_bytes=16_384,
        columns=[ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False)],
    )
    orders = TableInfo(
        name="orders",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=9_500,
        size_bytes=131_072,
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
    )
    return IntrospectionResult(
        database="atlas",
        engine="postgresql",
        host="localhost",
        schemas=[SchemaInfo(name="public", engine="postgresql", tables=[accounts, orders])],
    )


def test_hover_script_is_embedded_in_svg(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert "<script type=\"text/javascript\">" in svg


def test_hover_script_contains_tooltip_group(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert "atlas-tooltip" in svg


def test_hover_script_binds_node_wrappers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert ".system-node-wrap" in svg


def test_hover_script_binds_edge_wrappers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert ".system-edge-wrap" in svg


def test_hover_script_uses_data_attributes_from_nodes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert 'data-table="orders"' in svg
    assert "columnCount" in svg or "column_count" in svg


def test_hover_script_has_columns_detail_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert 'data-columns-detail="' in svg
    assert "columnsDetail" in svg


def test_hover_script_mentions_direct_relationships(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert 'data-relationship-kind="direct"' in svg
    assert "Direct relationship" in svg


def test_hover_script_exposes_schema_and_column_hover_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert 'class="system-schema-wrap"' in svg
    assert 'class="system-column-wrap"' in svg
    assert 'class="system-column-link-wrap"' in svg


def test_table_hover_wrap_does_not_swallow_internal_column_hover_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    node_groups = re.findall(r'(<g class="system-node-wrap".*?</g>)', svg, re.S)

    assert node_groups
    assert all('system-column-wrap' not in group for group in node_groups)
    assert all('system-column-link-wrap' not in group for group in node_groups)


def test_datamap_svg_uses_only_script_hover_without_native_title_tooltips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert "<script type=\"text/javascript\">" in svg
    assert "<title>" not in svg
