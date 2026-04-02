"""Phase 4C unit tests for native force-layout integration."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from atlas import _sigilo
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.sigilo.types import SigiloConfig
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)


def _result(node_count: int) -> IntrospectionResult:
    tables: list[TableInfo] = []
    for index in range(node_count):
        columns = [
            ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
        ]
        if index > 0:
            columns.append(
                ColumnInfo(
                    name=f"parent_{index}_id",
                    native_type="bigint",
                    is_nullable=False,
                    is_foreign_key=True,
                )
            )
        table = TableInfo(
            name=f"table_{index}",
            schema="public",
            table_type=TableType.TABLE,
            row_count_estimate=1_000 * (index + 1),
            size_bytes=4_096 * (index + 1),
            columns=columns,
        )
        if index > 0:
            table.foreign_keys = [
                ForeignKeyInfo(
                    name=f"fk_{index}",
                    source_schema="public",
                    source_table=f"table_{index}",
                    source_columns=[f"parent_{index}_id"],
                    target_schema="public",
                    target_table=f"table_{index - 1}",
                    target_columns=["id"],
                )
            ]
        tables.append(table)
    return IntrospectionResult(
        database="atlas",
        engine="postgresql",
        host="localhost",
        schemas=[SchemaInfo(name="public", engine="postgresql", tables=tables)],
    )


class _FakeContext:
    def __init__(self, config: SigiloConfig) -> None:
        self.config = config
        self.force_calls: list[tuple[int, float, float]] = []
        self.circular_calls = 0

    def __enter__(self) -> _FakeContext:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def add_node(self, node: Any) -> int:
        return 0

    def add_edge(self, edge: Any) -> bool:
        return True

    def compute_layout(self) -> None:
        self.circular_calls += 1

    def compute_layout_force(self, iterations: int, temperature: float, cooling: float) -> None:
        self.force_calls.append((iterations, temperature, cooling))

    def render(self, *, compute_layout: bool = True) -> bytes:
        return b"<svg></svg>"

    @property
    def error_message(self) -> str:
        return ""


def test_builder_uses_force_layout_for_large_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    contexts: list[_FakeContext] = []

    class _FakeSigiloModule:
        @staticmethod
        def available() -> bool:
            return True

        @staticmethod
        def RenderContext(config: SigiloConfig) -> _FakeContext:
            context = _FakeContext(config)
            contexts.append(context)
            return context

    monkeypatch.setattr("atlas.sigilo.builder._sigilo", _FakeSigiloModule)

    DatamapSigiloBuilder(_result(7)).set_layout("force").set_force_params(111, 0.8, 0.95).build()

    assert contexts[0].force_calls == [(111, 0.8, 0.95)]
    assert contexts[0].circular_calls == 0


def test_builder_falls_back_to_circular_layout_for_small_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contexts: list[_FakeContext] = []

    class _FakeSigiloModule:
        @staticmethod
        def available() -> bool:
            return True

        @staticmethod
        def RenderContext(config: SigiloConfig) -> _FakeContext:
            context = _FakeContext(config)
            contexts.append(context)
            return context

    monkeypatch.setattr("atlas.sigilo.builder._sigilo", _FakeSigiloModule)

    DatamapSigiloBuilder(_result(4)).set_layout("force").build()

    assert contexts[0].force_calls == []
    assert contexts[0].circular_calls == 1


def test_render_context_compute_layout_force_validates_iterations() -> None:
    ctx = object.__new__(_sigilo.RenderContext)
    ctx._lib = SimpleNamespace(atlas_render_compute_layout_force=lambda *_: None)
    ctx._ctx = SimpleNamespace(had_error=False)

    with pytest.raises(ValueError):
        ctx.compute_layout_force(iterations=0)


def test_render_context_compute_layout_force_validates_temperature() -> None:
    ctx = object.__new__(_sigilo.RenderContext)
    ctx._lib = SimpleNamespace(atlas_render_compute_layout_force=lambda *_: None)
    ctx._ctx = SimpleNamespace(had_error=False)

    with pytest.raises(ValueError):
        ctx.compute_layout_force(temperature=0.0)


def test_render_context_compute_layout_force_validates_cooling() -> None:
    ctx = object.__new__(_sigilo.RenderContext)
    ctx._lib = SimpleNamespace(atlas_render_compute_layout_force=lambda *_: None)
    ctx._ctx = SimpleNamespace(had_error=False)

    with pytest.raises(ValueError):
        ctx.compute_layout_force(cooling=1.0)


def test_render_context_compute_layout_force_falls_back_when_symbol_missing() -> None:
    ctx = object.__new__(_sigilo.RenderContext)
    ctx._lib = SimpleNamespace()
    ctx._ctx = SimpleNamespace(had_error=False)
    calls: list[str] = []
    ctx.compute_layout = lambda: calls.append("circular")  # type: ignore[method-assign]

    ctx.compute_layout_force()

    assert calls == ["circular"]
