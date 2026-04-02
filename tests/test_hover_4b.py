"""Phase 4B unit tests for embedded sigilo hover metadata."""

from __future__ import annotations

import pytest

from atlas.sigilo.builder import SigiloBuilder
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.sigilo.hover import HoverScriptBuilder
from atlas.types import ColumnInfo, IntrospectionResult, SchemaInfo, TableInfo, TableType


def _result() -> IntrospectionResult:
    return IntrospectionResult(
        database="atlas",
        engine="postgresql",
        host="localhost",
        schemas=[
            SchemaInfo(
                name="public",
                engine="postgresql",
                tables=[
                    TableInfo(
                        name="accounts",
                        schema="public",
                        table_type=TableType.TABLE,
                        row_count_estimate=1_000,
                        size_bytes=4_096,
                        columns=[
                            ColumnInfo(
                                name="id",
                                native_type="bigint",
                                is_primary_key=True,
                                is_nullable=False,
                            )
                        ],
                    )
                ],
            )
        ],
    )


def test_hover_script_builder_wraps_js_block() -> None:
    script = HoverScriptBuilder().build_script()

    assert script.startswith('<script type="text/javascript"><![CDATA[\n')
    assert script.endswith("\n]]></script>\n")


def test_hover_script_targets_system_wrappers() -> None:
    script = HoverScriptBuilder().build_script()

    assert ".system-node-wrap" in script
    assert ".system-edge-wrap" in script
    assert "atlas-tooltip" in script
    assert "document.currentScript" in script


def test_hover_script_exposes_formatters() -> None:
    script = HoverScriptBuilder().build_script()

    assert "_fmtRows" in script
    assert "_fmtBytes" in script
    assert "_fmtType" in script
    assert "_measureText" in script
    assert "_wrapText" in script
    assert "TIP_MAX_W = 420" in script
    assert "bold 12.65px monospace" in script
    assert "11.5px monospace" in script


def test_hover_script_parses_columns_detail_payload() -> None:
    script = HoverScriptBuilder().build_script()

    assert "_parseColumns" in script
    assert "columnsDetail" in script
    assert "more columns" in script


def test_hover_script_distinguishes_direct_and_indirect_relationships() -> None:
    script = HoverScriptBuilder().build_script()

    assert "Direct relationship" in script
    assert "Indirect relationship" in script


def test_hover_script_binds_schema_and_column_specific_targets() -> None:
    script = HoverScriptBuilder().build_script()

    assert ".system-schema-wrap" in script
    assert ".system-column-wrap" in script
    assert ".system-column-link-wrap" in script


def test_builder_inject_hover_script_before_svg_close() -> None:
    builder = SigiloBuilder(_result())

    svg = builder._inject_hover_script(b"<svg><g></g></svg>").decode("utf-8")

    assert svg.endswith("</svg>")
    assert "<script type=\"text/javascript\">" in svg
    assert svg.index("<script") < svg.index("</svg>")


def test_builder_does_not_modify_svg_without_closing_tag() -> None:
    builder = SigiloBuilder(_result())

    assert builder._inject_hover_script(b"<svg />") == b"<svg />"


def test_datamap_build_includes_hover_script(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_result()).build().decode("utf-8")

    assert "<script type=\"text/javascript\">" in svg
    assert "atlas-tooltip" in svg
