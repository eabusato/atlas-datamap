"""Phase 3B integration tests for the high-level sigilo builder."""

from __future__ import annotations

from pathlib import Path

import pytest

import atlas._sigilo as native_sigilo
from atlas.sigilo.builder import SigiloBuilder
from atlas.sigilo.types import SigiloConfig
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_3b]


def _result_fixture() -> IntrospectionResult:
    accounts = TableInfo(
        name="accounts",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=2_500,
        size_bytes=131_072,
        comment="Customer accounts",
        columns=[
            ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="email", native_type="varchar(255)", is_nullable=False),
        ],
    )
    invoices = TableInfo(
        name="invoices",
        schema="billing",
        table_type=TableType.TABLE,
        row_count_estimate=12_400,
        size_bytes=524_288,
        columns=[
            ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="account_id", native_type="bigint", is_nullable=False, is_foreign_key=True),
        ],
        foreign_keys=[
            ForeignKeyInfo(
                name="fk_invoices_account",
                source_schema="billing",
                source_table="invoices",
                source_columns=["account_id"],
                target_schema="public",
                target_table="accounts",
                target_columns=["id"],
                on_delete="RESTRICT",
            )
        ],
    )
    invoice_view = TableInfo(
        name="invoice_view",
        schema="billing",
        table_type=TableType.VIEW,
        row_count_estimate=500,
        size_bytes=8_192,
        columns=[ColumnInfo(name="invoice_id", native_type="bigint", is_nullable=False)],
    )
    return IntrospectionResult(
        database="billing",
        engine="postgresql",
        host="localhost",
        schemas=[
            SchemaInfo(name="public", engine="postgresql", tables=[accounts]),
            SchemaInfo(name="billing", engine="postgresql", tables=[invoices, invoice_view]),
        ],
    )


@pytest.fixture(scope="module")
def built_native_library(repo_root: Path) -> Path:
    if native_sigilo.available() and native_sigilo.library_path():
        return Path(native_sigilo.library_path())
    result = pytest.importorskip("subprocess").run(
        ["make", "-C", str(repo_root / "atlas" / "_c"), "clean", "all"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"Unable to build native sigilo library: {result.stderr[:300]}")
    library_path = native_sigilo._find_library()  # type: ignore[attr-defined]
    if library_path is None:
        pytest.skip("Native sigilo library could not be located after build")
    native_sigilo._load()  # type: ignore[attr-defined]
    return Path(library_path)


def test_builder_native_path_emits_svg(built_native_library: Path) -> None:
    assert built_native_library.exists()
    svg = SigiloBuilder(_result_fixture()).build_svg().decode("utf-8")
    assert "<svg" in svg
    assert "data-table=\"accounts\"" in svg


def test_builder_emits_foreign_key_metadata() -> None:
    svg = SigiloBuilder(_result_fixture()).build_svg().decode("utf-8")
    assert "data-fk-from=\"billing.invoices\"" in svg
    assert "data-fk-to=\"public.accounts\"" in svg
    assert "data-fk-type=\"declared\"" in svg


def test_builder_emits_schema_labels_for_multiple_schemas() -> None:
    svg = SigiloBuilder(_result_fixture()).build_svg().decode("utf-8")
    assert "public" in svg
    assert "billing" in svg
    assert "schema_rings" in svg


def test_builder_python_fallback_can_be_forced() -> None:
    svg = SigiloBuilder(_result_fixture(), prefer_native=False).build_svg().decode("utf-8")
    assert "<svg" in svg
    assert "invoice_view" in svg


def test_builder_respects_config_flags_in_fallback() -> None:
    svg = SigiloBuilder(
        _result_fixture(),
        config=SigiloConfig(emit_data_attrs=False, emit_titles=False),
        prefer_native=False,
    ).build_svg().decode("utf-8")
    assert "<svg" in svg
    assert "data-table=" not in svg
    assert "<title>" not in svg
