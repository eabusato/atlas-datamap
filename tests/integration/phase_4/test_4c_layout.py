"""Phase 4C integration tests for native force-directed layout."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

import atlas._sigilo as native_sigilo
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_4c]


def _result(node_count: int, *, multi_schema: bool = False) -> IntrospectionResult:
    schemas: dict[str, list[TableInfo]] = {"public": []}
    if multi_schema:
        schemas["analytics"] = []
    for index in range(node_count):
        schema_name = "analytics" if multi_schema and index % 3 == 0 else "public"
        table = TableInfo(
            name=f"table_{index}",
            schema=schema_name,
            table_type=TableType.TABLE,
            row_count_estimate=1_000 * (index + 1),
            size_bytes=8_192 * (index + 1),
            columns=[
                ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
                ColumnInfo(
                    name=f"parent_{index}_id",
                    native_type="bigint",
                    is_nullable=False,
                    is_foreign_key=index > 0,
                ),
            ],
        )
        if index > 0:
            prev_schema = "analytics" if multi_schema and (index - 1) % 3 == 0 else "public"
            table.foreign_keys = [
                ForeignKeyInfo(
                    name=f"fk_{index}",
                    source_schema=schema_name,
                    source_table=f"table_{index}",
                    source_columns=[f"parent_{index}_id"],
                    target_schema=prev_schema,
                    target_table=f"table_{index - 1}",
                    target_columns=["id"],
                )
            ]
        schemas[schema_name].append(table)
    return IntrospectionResult(
        database="atlas",
        engine="postgresql",
        host="localhost",
        schemas=[
            SchemaInfo(name=schema_name, engine="postgresql", tables=tables)
            for schema_name, tables in schemas.items()
        ],
    )


def _extract_positions(svg: str) -> list[tuple[str, float, float]]:
    pattern = re.compile(
        r'<g class="system-node-wrap"[^>]*data-table="([^"]+)"[^>]*>\s*<circle class="[^"]+" cx="([0-9.]+)" cy="([0-9.]+)"',
        re.MULTILINE,
    )
    return [(match.group(1), float(match.group(2)), float(match.group(3))) for match in pattern.finditer(svg)]


@pytest.fixture(scope="module")
def built_native_library(repo_root: Path) -> Path:
    result = subprocess.run(
        ["make", "-C", str(repo_root / "atlas" / "_c"), "clean", "all"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"Unable to build native sigilo library: {result.stderr[:300]}")
    native_sigilo._load()  # type: ignore[attr-defined]
    path = native_sigilo.library_path()
    if not path:
        pytest.skip("Native sigilo library could not be loaded after build")
    return Path(path)


def test_force_layout_builds_svg_with_native_library(built_native_library: Path) -> None:
    assert built_native_library.exists()

    svg = DatamapSigiloBuilder(_result(8)).set_layout("force").build().decode("utf-8")

    assert "<svg" in svg
    assert 'class="system-node-wrap"' in svg
    assert "<script type=\"text/javascript\">" in svg


def test_force_layout_changes_positions_for_large_graph(built_native_library: Path) -> None:
    circular_svg = DatamapSigiloBuilder(_result(8)).set_layout("circular").build().decode("utf-8")
    force_svg = DatamapSigiloBuilder(_result(8)).set_layout("force").build().decode("utf-8")

    assert _extract_positions(force_svg) != _extract_positions(circular_svg)


def test_force_layout_falls_back_to_circular_for_small_graph(built_native_library: Path) -> None:
    circular_svg = DatamapSigiloBuilder(_result(4)).set_layout("circular").build().decode("utf-8")
    force_svg = DatamapSigiloBuilder(_result(4)).set_layout("force").build().decode("utf-8")

    assert _extract_positions(force_svg) == _extract_positions(circular_svg)


def test_force_layout_keeps_nodes_inside_canvas(built_native_library: Path) -> None:
    svg = DatamapSigiloBuilder(_result(10)).set_layout("force").build().decode("utf-8")
    positions = _extract_positions(svg)

    assert positions
    assert all(0.0 < x < 1200.0 and 0.0 < y < 1200.0 for _, x, y in positions)


def test_force_layout_preserves_multi_schema_rings(built_native_library: Path) -> None:
    svg = DatamapSigiloBuilder(_result(9, multi_schema=True)).set_layout("force").build().decode("utf-8")

    assert 'id="schema_rings"' in svg
    assert "public" in svg
    assert "analytics" in svg
