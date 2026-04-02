"""Phase 4A unit tests for the datamap-oriented sigilo builder."""

from __future__ import annotations

import math
import re

import pytest

from atlas.sigilo.datamap import (
    DatamapSigiloBuilder,
    _build_row_scale_profile,
    _column_to_desc,
    _compute_node_radius,
    _make_node_id,
    _row_percentile_factor,
    _table_type_to_node_type,
)
from atlas.sigilo.style import SigiloStyle, get_style_params
from atlas.types import (
    ColumnInfo,
    ColumnStats,
    ForeignKeyInfo,
    IndexInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)


def _sample_result() -> IntrospectionResult:
    accounts = TableInfo(
        name="accounts",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=1_000,
        size_bytes=16_384,
        comment="Customer accounts",
        columns=[
            ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="email", native_type="varchar(255)", is_nullable=False),
        ],
        indexes=[
            IndexInfo(name="accounts_pkey", table="accounts", schema="public", columns=["id"], is_primary=True),
        ],
    )
    orders = TableInfo(
        name="orders-v1",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=125_000,
        size_bytes=524_288,
        comment="Customer orders",
        columns=[
            ColumnInfo(name="id", native_type="bigint", is_primary_key=True, is_nullable=False),
            ColumnInfo(
                name="account_id",
                native_type="bigint",
                is_nullable=False,
                is_foreign_key=True,
                stats=ColumnStats(row_count=125_000, distinct_count=1_500),
            ),
        ],
        foreign_keys=[
            ForeignKeyInfo(
                name="fk_orders_account",
                source_schema="public",
                source_table="orders-v1",
                source_columns=["account_id"],
                target_schema="public",
                target_table="accounts",
                target_columns=["id"],
                on_delete="CASCADE",
            )
        ],
        indexes=[
            IndexInfo(name="orders_pkey", table="orders-v1", schema="public", columns=["id"], is_primary=True),
            IndexInfo(name="idx_orders_account", table="orders-v1", schema="public", columns=["account_id"]),
        ],
    )
    invoice_view = TableInfo(
        name="invoice.view",
        schema="reporting-app",
        table_type=TableType.VIEW,
        row_count_estimate=240,
        size_bytes=4_096,
        columns=[ColumnInfo(name="invoice_id", native_type="bigint", is_nullable=False)],
    )
    invoice_rollup = TableInfo(
        name="invoice_rollup",
        schema="reporting-app",
        table_type=TableType.MATERIALIZED_VIEW,
        row_count_estimate=4_200,
        size_bytes=32_768,
        columns=[ColumnInfo(name="day_bucket", native_type="date", is_nullable=False)],
    )
    ghost_table = TableInfo(
        name="ghost_orders",
        schema="foreign-data",
        table_type=TableType.FOREIGN_TABLE,
        row_count_estimate=32,
        size_bytes=1_024,
        columns=[ColumnInfo(name="remote_id", native_type="integer", is_nullable=False)],
        foreign_keys=[
            ForeignKeyInfo(
                name="fk_ghost_orders_accounts",
                source_schema="foreign-data",
                source_table="ghost_orders",
                source_columns=["remote_id"],
                target_schema="public",
                target_table="accounts",
                target_columns=["id"],
                is_inferred=True,
            )
        ],
    )
    return IntrospectionResult(
        database="atlas",
        engine="postgresql",
        host="localhost",
        schemas=[
            SchemaInfo(name="public", engine="postgresql", tables=[accounts, orders]),
            SchemaInfo(name="reporting-app", engine="postgresql", tables=[invoice_view, invoice_rollup]),
            SchemaInfo(name="foreign-data", engine="postgresql", tables=[ghost_table]),
        ],
    )


@pytest.mark.parametrize(
    ("table_type", "expected"),
    [
        (TableType.TABLE, "table"),
        (TableType.VIEW, "view"),
        (TableType.MATERIALIZED_VIEW, "materialized_view"),
        (TableType.FOREIGN_TABLE, "foreign_table"),
        (TableType.SYNONYM, "view"),
    ],
)
def test_table_type_to_node_type(table_type: TableType, expected: str) -> None:
    assert _table_type_to_node_type(table_type) == expected


def test_make_node_id_normalizes_dots_and_dashes() -> None:
    assert _make_node_id("reporting-app", "invoice.view") == "reporting_app.invoice_view"


def test_column_to_desc_maps_stats_and_flags() -> None:
    column = ColumnInfo(
        name="account_id",
        native_type="bigint",
        is_nullable=False,
        is_foreign_key=True,
        stats=ColumnStats(row_count=10, null_count=2, distinct_count=5),
    )

    desc = _column_to_desc(column)

    assert desc.name == "account_id"
    assert desc.type_str == "bigint"
    assert desc.is_fk is True
    assert desc.is_nullable is False
    assert desc.distinct_estimate == 5
    assert desc.null_rate == pytest.approx(0.2)


def test_compute_node_radius_log_clamps_range() -> None:
    radius = _compute_node_radius(100, 8.0, 32.0, 1_000_000.0, "log")
    assert 8.0 <= radius <= 32.0


def test_compute_node_radius_linear_hits_max() -> None:
    assert _compute_node_radius(2_000, 8.0, 32.0, 1_000.0, "linear") == pytest.approx(32.0)


def test_compute_node_radius_sqrt_is_between_min_and_max() -> None:
    radius = _compute_node_radius(250_000, 8.0, 32.0, 1_000_000.0, "sqrt")
    assert 8.0 < radius < 32.0


def test_row_scale_profile_uses_current_database_distribution() -> None:
    profile = _build_row_scale_profile([4_000, 5_000, 6_000, 50_000], default_reference_rows=1_000_000.0)

    assert profile.reference_rows == 50_000
    assert profile.minimum_rows == 4_000
    assert profile.mean_rows == pytest.approx(16_250.0)


def test_row_percentile_factor_tracks_relative_rank() -> None:
    profile = _build_row_scale_profile([5, 50, 500, 5_000], default_reference_rows=1_000_000.0)

    assert _row_percentile_factor(5, profile) == pytest.approx(0.0)
    assert _row_percentile_factor(5_000, profile) == pytest.approx(1.0)
    assert _row_percentile_factor(500, profile) > _row_percentile_factor(50, profile)


def test_compute_node_radius_emphasizes_relative_differences_around_bank_center() -> None:
    profile = _build_row_scale_profile([4_000, 5_000, 6_000], default_reference_rows=1_000_000.0)

    smaller = _compute_node_radius(4_000, 30.0, 164.0, 1_000_000.0, "log", profile=profile)
    larger = _compute_node_radius(6_000, 30.0, 164.0, 1_000_000.0, "log", profile=profile)

    assert larger > smaller
    assert larger - smaller > 20.0


def test_compute_node_radius_distinguishes_large_tables_more_aggressively() -> None:
    profile = _build_row_scale_profile([5, 50, 500, 5_000, 2_000_000, 5_000_000], default_reference_rows=1_000_000.0)

    medium_large = _compute_node_radius(2_000_000, 30.0, 164.0, 1_000_000.0, "log", profile=profile)
    very_large = _compute_node_radius(5_000_000, 30.0, 164.0, 1_000_000.0, "log", profile=profile)

    assert very_large > medium_large
    assert very_large - medium_large > 10.0


def test_sigilo_style_from_string_is_case_insensitive() -> None:
    assert SigiloStyle.from_str("Compact") is SigiloStyle.COMPACT


def test_sigilo_style_from_string_rejects_invalid_value() -> None:
    with pytest.raises(ValueError):
        SigiloStyle.from_str("loud")


def test_get_style_params_compact_canvas() -> None:
    params = get_style_params(SigiloStyle.COMPACT)
    assert params.canvas_w == 800.0
    assert params.emit_macro_rings is False


def test_builder_collects_all_nodes_and_schemas() -> None:
    nodes, edges, schema_names = DatamapSigiloBuilder(_sample_result())._collect()

    assert len(nodes) == 5
    assert len(edges) == 2
    assert schema_names == ["public", "reporting-app", "foreign-data"]


def test_builder_collects_node_counts_and_radius() -> None:
    nodes, _, _ = DatamapSigiloBuilder(_sample_result())._collect()
    accounts = next(node for node in nodes if node.name == "accounts")
    orders = next(node for node in nodes if node.name == "orders-v1")

    assert orders.fk_count == 1
    assert orders.index_count == 2
    assert orders.r > 0
    assert orders.r > accounts.r


def test_builder_collects_declared_and_inferred_edges() -> None:
    _, edges, _ = DatamapSigiloBuilder(_sample_result())._collect()

    assert {edge.edge_type for edge in edges} == {"declared", "inferred"}


def test_builder_schema_filter_removes_filtered_nodes_and_edges() -> None:
    builder = DatamapSigiloBuilder(_sample_result()).set_schema_filter(["public"])
    nodes, edges, schema_names = builder._collect()

    assert schema_names == ["public"]
    assert {node.name for node in nodes} == {"accounts", "orders-v1"}
    assert len(edges) == 1
    assert edges[0].to_id == "public.accounts"


def test_builder_schema_filter_none_keeps_all_schemas() -> None:
    builder = DatamapSigiloBuilder(_sample_result()).set_schema_filter(None)
    nodes, _, schema_names = builder._collect()

    assert len(nodes) == 5
    assert len(schema_names) == 3


def test_builder_build_config_uses_style_params() -> None:
    builder = DatamapSigiloBuilder(_sample_result()).set_style("seal")
    config = builder._build_config(get_style_params(SigiloStyle.SEAL), ["public"])

    assert config.canvas_w == 512.0
    assert config.ring_stroke_dash == "6 4"
    assert config.font_hash == "8px monospace"
    assert config.emit_titles is False


def test_render_places_schema_centers_on_a_shared_outer_orbit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_sample_result()).build().decode("utf-8")

    core = re.search(r'<circle class="db-core" cx="([0-9.]+)" cy="([0-9.]+)"', svg)
    schemas = re.findall(
        r'<g class="system-schema-wrap" data-schema="[^"]+".*?<circle class="macro-ring" cx="([0-9.]+)" cy="([0-9.]+)" r="([0-9.]+)"/>',
        svg,
        re.S,
    )

    assert core is not None
    assert len(schemas) == 3

    cx = float(core.group(1))
    cy = float(core.group(2))
    distances = [math.hypot(float(x) - cx, float(y) - cy) for x, y, _ in schemas]
    assert max(distances) - min(distances) < 1.0


def test_render_avoids_schema_ring_overlap_for_default_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_sample_result()).build().decode("utf-8")

    schemas = [
        (float(cx), float(cy), float(r))
        for cx, cy, r in re.findall(
            r'<g class="system-schema-wrap" data-schema="[^"]+".*?<circle class="macro-ring" cx="([0-9.]+)" cy="([0-9.]+)" r="([0-9.]+)"/>',
            svg,
            re.S,
        )
    ]

    assert len(schemas) == 3
    for index, (x1, y1, r1) in enumerate(schemas):
        for x2, y2, r2 in schemas[index + 1 :]:
            assert math.hypot(x1 - x2, y1 - y2) > (r1 + r2)


def test_builder_size_scale_validation() -> None:
    with pytest.raises(ValueError):
        DatamapSigiloBuilder(_sample_result()).set_size_scale("cubic")  # type: ignore[arg-type]


def test_builder_layout_validation() -> None:
    with pytest.raises(ValueError):
        DatamapSigiloBuilder(_sample_result()).set_layout("spiral")  # type: ignore[arg-type]


def test_builder_force_params_validation_iterations() -> None:
    with pytest.raises(ValueError):
        DatamapSigiloBuilder(_sample_result()).set_force_params(iterations=0)


def test_builder_force_params_validation_temperature() -> None:
    with pytest.raises(ValueError):
        DatamapSigiloBuilder(_sample_result()).set_force_params(temperature=0.0)


def test_builder_force_params_validation_cooling() -> None:
    with pytest.raises(ValueError):
        DatamapSigiloBuilder(_sample_result()).set_force_params(cooling=1.0)


def test_builder_build_returns_non_empty_svg_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_sample_result()).build().decode("utf-8")

    assert svg.startswith("<svg")
    assert 'class="system-node-wrap"' in svg
    assert 'class="system-edge-wrap"' in svg


def test_builder_build_contains_expected_css_classes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_sample_result()).build().decode("utf-8")

    assert 'class="node-aux"' in svg
    assert 'class="node-loop"' in svg
    assert 'class="call"' in svg
    assert 'class="branch"' in svg
    assert 'class="col-pk"' in svg


def test_builder_build_contains_database_foundation_and_core_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_sample_result()).build().decode("utf-8")

    assert 'id="database_foundation"' in svg
    assert "atlas" in svg
    assert "postgresql @ localhost" in svg


def test_builder_build_contains_internal_table_structure_and_hover_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_sample_result()).build().decode("utf-8")

    assert 'class="node-shell"' in svg
    assert 'class="node-core"' in svg
    assert 'class="col-orbit"' in svg
    assert 'class="col-spoke"' in svg
    assert 'class="col-chord"' in svg
    assert 'data-columns-detail="' in svg


def test_builder_build_marks_relationship_kind_for_hover(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_sample_result()).build().decode("utf-8")

    assert 'data-relationship-kind="direct"' in svg
    assert 'data-relationship-kind="indirect"' in svg


def test_builder_compact_style_uses_800_canvas(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_sample_result()).set_style("compact").build().decode("utf-8")

    width = re.search(r'width="(\d+)"', svg)
    height = re.search(r'height="(\d+)"', svg)
    assert width is not None and int(width.group(1)) >= 800
    assert height is not None and int(height.group(1)) >= 800
    assert 'id="schema_rings"' not in svg


def test_builder_schema_filter_excludes_other_schema_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = (
        DatamapSigiloBuilder(_sample_result())
        .set_schema_filter(["public"])
        .build()
        .decode("utf-8")
    )

    assert "Customer orders" in svg
    assert "invoice_rollup" not in svg
    assert "foreign-data" not in svg


def test_builder_build_raises_for_empty_result() -> None:
    empty = IntrospectionResult(database="atlas", engine="postgresql", host="localhost", schemas=[])

    with pytest.raises(RuntimeError):
        DatamapSigiloBuilder(empty).build()


def test_svg_contains_numeric_row_and_size_attributes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    svg = DatamapSigiloBuilder(_sample_result()).build().decode("utf-8")

    assert re.search(r'data-row-estimate="-?\d+"', svg)
    assert re.search(r'data-size-bytes="-?\d+"', svg)


def test_network_style_keeps_macro_rings_enabled() -> None:
    config = DatamapSigiloBuilder(_sample_result())._build_config(
        get_style_params(SigiloStyle.NETWORK),
        ["public", "reporting-app"],
    )

    assert config.emit_macro_rings is True
    assert config.schema_orbit_r == 450.0
