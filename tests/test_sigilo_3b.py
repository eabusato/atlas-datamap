"""Phase 3B unit tests for the Python sigilo wrapper and builder."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

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


def _sample_result() -> IntrospectionResult:
    customers = TableInfo(
        name="customers",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=1200,
        size_bytes=32_768,
        comment="Customer master",
        columns=[
            ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="email", native_type="varchar(255)", is_nullable=False),
        ],
    )
    orders = TableInfo(
        name="orders",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=42_000,
        size_bytes=262_144,
        comment="Customer orders",
        columns=[
            ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
            ColumnInfo(
                name="customer_id",
                native_type="bigint",
                is_nullable=False,
                is_foreign_key=True,
            ),
        ],
        foreign_keys=[
            ForeignKeyInfo(
                name="fk_orders_customer",
                source_schema="public",
                source_table="orders",
                source_columns=["customer_id"],
                target_schema="public",
                target_table="customers",
                target_columns=["id"],
                on_delete="CASCADE",
            )
        ],
    )
    report = TableInfo(
        name="sales_report",
        schema="analytics",
        table_type=TableType.MATERIALIZED_VIEW,
        row_count_estimate=500,
        size_bytes=16_384,
        columns=[ColumnInfo(name="day", native_type="date", is_nullable=False)],
    )
    return IntrospectionResult(
        database="demo",
        engine="postgresql",
        host="localhost",
        schemas=[
            SchemaInfo(name="public", engine="postgresql", tables=[customers, orders]),
            SchemaInfo(name="analytics", engine="postgresql", tables=[report]),
        ],
    )


def test_builder_maps_tables_to_sigilo_nodes() -> None:
    builder = SigiloBuilder(_sample_result())
    nodes = builder.build_nodes()

    assert [node.id for node in nodes] == [
        "public.customers",
        "public.orders",
        "analytics.sales_report",
    ]
    assert nodes[2].node_type == "materialized_view"
    assert nodes[1].comment == "Customer orders"


def test_builder_maps_foreign_keys_to_sigilo_edges() -> None:
    builder = SigiloBuilder(_sample_result())
    edges = builder.build_edges()

    assert len(edges) == 1
    assert edges[0].from_id == "public.orders"
    assert edges[0].to_id == "public.customers"
    assert edges[0].edge_type == "declared"
    assert edges[0].on_delete == "CASCADE"


@dataclass
class _FakeContext:
    config: SigiloConfig
    nodes: list[str] = field(default_factory=list)
    edges: list[str] = field(default_factory=list)

    def __enter__(self) -> _FakeContext:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def add_node(self, node: Any) -> int:
        self.nodes.append(node.id)
        return len(self.nodes) - 1

    def add_edge(self, edge: Any) -> bool:
        self.edges.append(f"{edge.from_id}->{edge.to_id}")
        return True

    def render(self) -> bytes:
        return b"<svg data-source='native'/>"

    @property
    def error_message(self) -> str:
        return ""


def test_builder_uses_native_renderer_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
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

    svg = SigiloBuilder(_sample_result()).build_svg()

    assert svg == b"<svg data-source='native'/>"
    assert contexts[0].nodes == ["public.customers", "public.orders", "analytics.sales_report"]
    assert contexts[0].edges == ["public.orders->public.customers"]


def test_builder_falls_back_when_native_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeSigiloModule:
        @staticmethod
        def available() -> bool:
            return False

    def _fake_render_svg(nodes: list[Any], edges: list[Any], config: SigiloConfig) -> bytes:
        captured["nodes"] = [node.id for node in nodes]
        captured["edges"] = [edge.from_id for edge in edges]
        captured["config"] = config.style
        return b"<svg data-source='python'/>"

    monkeypatch.setattr("atlas.sigilo.builder._sigilo", _FakeSigiloModule)
    monkeypatch.setattr("atlas.sigilo.builder._python_fallback.render_svg", _fake_render_svg)

    svg = SigiloBuilder(_sample_result(), config=SigiloConfig(style="compact")).build_svg()

    assert svg == b"<svg data-source='python'/>"
    assert captured["nodes"] == ["public.customers", "public.orders", "analytics.sales_report"]
    assert captured["edges"] == ["public.orders"]
    assert captured["config"] == "compact"


def test_builder_falls_back_when_native_renderer_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    class _BoomContext:
        def __enter__(self) -> _BoomContext:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def add_node(self, node: Any) -> int:
            return 0

        def add_edge(self, edge: Any) -> bool:
            return True

        def render(self) -> bytes:
            raise RuntimeError("boom")

        @property
        def error_message(self) -> str:
            return "boom"

    class _FakeSigiloModule:
        @staticmethod
        def available() -> bool:
            return True

        @staticmethod
        def RenderContext(config: SigiloConfig) -> _BoomContext:
            return _BoomContext()

    monkeypatch.setattr("atlas.sigilo.builder._sigilo", _FakeSigiloModule)
    monkeypatch.setattr(
        "atlas.sigilo.builder._python_fallback.render_svg",
        lambda nodes, edges, config: b"<svg data-source='fallback-after-error'/>",
    )

    svg = SigiloBuilder(_sample_result()).build_svg()

    assert svg == b"<svg data-source='fallback-after-error'/>"
