"""Integration tests for Phase 9B semantic sigilo enrichment."""

from __future__ import annotations

import pytest

from atlas.sigilo import builder as sigilo_builder_module
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_9b]


def _semantic_result() -> IntrospectionResult:
    accounts = TableInfo(
        name="accounts",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=1_250,
        size_bytes=16_384,
        semantic_short="Accounts",
        semantic_detailed="Customer account master data.",
        semantic_domain="crm",
        semantic_role="dimension",
        semantic_confidence=0.91,
        columns=[
            ColumnInfo(
                name="id",
                native_type="bigint",
                is_primary_key=True,
                is_nullable=False,
                semantic_short="Account identifier",
                semantic_detailed="Stable technical identifier.",
                semantic_role="identifier",
                semantic_confidence=0.98,
            ),
            ColumnInfo(
                name="email",
                native_type="varchar(255)",
                is_nullable=False,
                semantic_short='Primary "email" <login>',
                semantic_detailed="Primary login address for the account.",
                semantic_role="email",
                semantic_confidence=0.94,
            ),
        ],
    )
    orders = TableInfo(
        name="orders",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=9_500,
        size_bytes=131_072,
        semantic_short="Orders",
        semantic_detailed="Commercial order headers.",
        semantic_domain="sales",
        semantic_role="transaction_header",
        semantic_confidence=0.88,
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


def test_semantic_table_attrs_are_injected_into_svg(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_semantic_result()).build().decode("utf-8")

    assert 'data-table="accounts"' in svg
    assert 'data-semantic-short="Accounts"' in svg
    assert 'data-semantic-detailed="Customer account master data."' in svg
    assert 'data-semantic-domain="crm"' in svg
    assert 'data-semantic-role="dimension"' in svg
    assert 'data-semantic-confidence="0.91"' in svg


def test_semantic_column_attrs_are_injected_and_escaped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_semantic_result()).build().decode("utf-8")

    assert 'data-column-name="email"' in svg
    assert 'data-semantic-short="Primary &quot;email&quot; &lt;login&gt;"' in svg
    assert 'data-semantic-role="email"' in svg
    assert 'data-semantic-confidence="0.94"' in svg


def test_semantic_hover_script_reads_semantic_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_semantic_result()).build().decode("utf-8")

    assert "semanticShort" in svg
    assert "semanticDetailed" in svg
    assert "semanticDomain" in svg
    assert "semanticRole" in svg
    assert "confidence:" in svg


def test_semantic_injection_preserves_existing_structural_attrs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_semantic_result()).build().decode("utf-8")

    assert 'data-column-count="2"' in svg
    assert 'data-fk-count="1"' in svg
    assert 'data-index-count="0"' in svg
    assert 'data-columns-detail="' in svg


def test_rebuild_with_semantics_uses_updated_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    result = _semantic_result()
    builder = DatamapSigiloBuilder(result)
    initial_svg = builder.build().decode("utf-8")
    assert 'data-semantic-short="Orders"' in initial_svg

    result.get_table("public", "orders").semantic_short = "Orders updated"
    rebuilt_svg = builder.rebuild_with_semantics(result).decode("utf-8")
    assert 'data-semantic-short="Orders updated"' in rebuilt_svg


def test_native_and_fallback_emit_same_semantic_attrs_when_available() -> None:
    if not sigilo_builder_module._sigilo.available():
        pytest.skip("Native sigilo renderer is not available in this environment.")

    result = _semantic_result()
    native_svg = DatamapSigiloBuilder(result).build().decode("utf-8")

    original = sigilo_builder_module._sigilo.available
    try:
        sigilo_builder_module._sigilo.available = lambda: False
        fallback_svg = DatamapSigiloBuilder(_semantic_result()).build().decode("utf-8")
    finally:
        sigilo_builder_module._sigilo.available = original

    for fragment in [
        'data-semantic-short="Accounts"',
        'data-semantic-detailed="Customer account master data."',
        'data-semantic-domain="crm"',
        'data-semantic-role="dimension"',
        'data-semantic-confidence="0.91"',
        'data-semantic-role="email"',
    ]:
        assert fragment in native_svg
        assert fragment in fallback_svg
