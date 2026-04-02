"""Pure-Python fallback renderer for sigilo SVG output."""

from __future__ import annotations

import math
import re
from collections import defaultdict
from dataclasses import dataclass
from html import escape
from typing import Any

from atlas.sigilo.types import SigiloColumnDesc, SigiloConfig, SigiloEdge, SigiloNode

_MIN_CANVAS = 512.0
_COL_DOT_RADIUS = 3.0
_COL_DOT_GAP = 8.0
_MAX_COLUMN_LABELS = 12
_COLUMN_BAND_SIZE = 10
_SCHEMA_GAP = 72.0
_SCHEMA_CANVAS_MARGIN = 220.0
_SCHEMA_LABEL_DEPTH = 48.0
_FONT_SCALE = 1.15


@dataclass(slots=True)
class _PlacedNode:
    node: SigiloNode
    cx: float
    cy: float
    r: float


@dataclass(slots=True)
class _SchemaLevel:
    name: str
    orbit_r: float
    support_r: float
    nodes: list[SigiloNode]


@dataclass(slots=True)
class _PlacedSchema:
    name: str
    cx: float
    cy: float
    support_r: float
    table_count: int
    direct_fk_count: int
    total_rows: int


def _schema_outer_orbit(
    levels: list[_SchemaLevel],
    config: SigiloConfig,
    width: float,
    height: float,
) -> float:
    if not levels:
        return config.schema_orbit_r

    if not config.emit_macro_rings:
        if len(levels) == 1:
            return 0.0
        max_support = max(level.support_r for level in levels)
        canvas_half = min(width, height) * 0.5
        available = max(0.0, canvas_half - max_support - 48.0)
        return min(config.schema_orbit_r * 0.55, available)

    max_support = max(level.support_r for level in levels)
    outer_orbit = max(config.schema_orbit_r, max_support + 320.0)
    if len(levels) == 1:
        return outer_orbit

    angle_step = 2.0 * math.pi / len(levels)
    chord_factor = 2.0 * math.sin(angle_step / 2.0)
    if chord_factor <= 0.0:
        return outer_orbit

    pairwise_requirement = 0.0
    for index, level in enumerate(levels):
        next_level = levels[(index + 1) % len(levels)]
        required = (level.support_r + next_level.support_r + _SCHEMA_GAP) / chord_factor
        pairwise_requirement = max(pairwise_requirement, required)
    return max(outer_orbit, pairwise_requirement)


def _fmt_px(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _scale_font_spec(spec: str, scale: float = _FONT_SCALE) -> str:
    match = re.search(r"(\d+(?:\.\d+)?)px", spec)
    if match is None:
        return spec
    scaled = _fmt_px(float(match.group(1)) * scale)
    return f"{spec[:match.start(1)]}{scaled}{spec[match.end(1):]}"


def render_svg(
    nodes: list[SigiloNode],
    edges: list[SigiloEdge],
    config: SigiloConfig,
    layout: str = "circular",
    force_params: dict[str, Any] | None = None,
) -> bytes:
    """Render nodes and edges into SVG using the Python fallback path."""

    width, height = _resolve_canvas(config)
    width, height = _expand_canvas_for_layout(nodes, config, width, height)
    if layout == "force" and len(nodes) > 5:
        placed_nodes = _place_nodes_force(nodes, config, width, height, force_params or {})
        placed_schemas = _derive_schema_positions(placed_nodes, config)
    else:
        placed_nodes, placed_schemas = _place_nodes_circular(nodes, config, width, height)
    node_map = {placed.node.id: placed for placed in placed_nodes}

    parts = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0f}" height="{height:.0f}" '
            f'viewBox="0 0 {width:.0f} {height:.0f}" role="img" aria-label="Atlas database sigilo">'
        ),
        "  <desc>Atlas Datamap - Python fallback sigilo.</desc>",
        _stylesheet(config),
        f'  <rect class="bg" width="{width:.0f}" height="{height:.0f}"/>',
        _emit_database_foundation(width, height, config),
        _emit_schema_labels(placed_schemas, config),
        _emit_edges(edges, node_map, config),
        _emit_nodes(placed_nodes, config),
        "</svg>",
    ]
    return "\n".join(part for part in parts if part).encode("utf-8")


render_svg_fallback = render_svg


def _resolve_canvas(config: SigiloConfig) -> tuple[float, float]:
    width = config.canvas_w if config.canvas_w > 0 else (config.canvas_width or _MIN_CANVAS)
    height = config.canvas_h if config.canvas_h > 0 else (config.canvas_height or _MIN_CANVAS)
    return max(_MIN_CANVAS, width), max(_MIN_CANVAS, height)


def _node_radius(config: SigiloConfig, node: SigiloNode) -> float:
    if node.r > 0:
        return node.r
    if node.row_estimate is None or node.row_estimate <= 0:
        return config.node_r_min
    scale = math.log1p(float(node.row_estimate)) / math.log1p(float(config.node_r_ref_rows))
    clamped = min(1.0, max(0.0, scale))
    return config.node_r_min + (config.node_r_max - config.node_r_min) * clamped


def _place_nodes_circular(
    nodes: list[SigiloNode],
    config: SigiloConfig,
    width: float,
    height: float,
) -> tuple[list[_PlacedNode], list[_PlacedSchema]]:
    if not nodes:
        return [], []

    grouped = _group_nodes_by_schema(nodes)
    center_x = width / 2.0
    center_y = height / 2.0
    levels = _compute_schema_levels(grouped, config)

    placed: list[_PlacedNode] = []
    placed_schemas: list[_PlacedSchema] = []
    cluster_orbit = _schema_outer_orbit(levels, config, width, height)
    for schema_index, level in enumerate(levels):
        schema_angle = 2.0 * math.pi * schema_index / max(len(levels), 1) - math.pi / 2.0
        schema_cx = center_x + cluster_orbit * math.cos(schema_angle)
        schema_cy = center_y + cluster_orbit * math.sin(schema_angle)
        schema_nodes = level.nodes
        angle_offset = -math.pi / 2.0 + schema_index * 0.28
        placed_schemas.append(
            _PlacedSchema(
                name=level.name,
                cx=schema_cx,
                cy=schema_cy,
                support_r=level.support_r,
                table_count=len(schema_nodes),
                direct_fk_count=sum(node.fk_count for node in schema_nodes),
                total_rows=sum(max(0, node.row_estimate or 0) for node in schema_nodes),
            )
        )
        for node_index, node in enumerate(schema_nodes):
            radius = _node_radius(config, node)
            if len(schema_nodes) == 1:
                cx = schema_cx
                cy = schema_cy - level.orbit_r
            else:
                node_angle = angle_offset + 2.0 * math.pi * node_index / len(schema_nodes)
                cx = schema_cx + level.orbit_r * math.cos(node_angle)
                cy = schema_cy + level.orbit_r * math.sin(node_angle)
            placed.append(_PlacedNode(node=node, cx=cx, cy=cy, r=radius))
    return placed, placed_schemas


def _group_nodes_by_schema(nodes: list[SigiloNode]) -> dict[str, list[SigiloNode]]:
    grouped: dict[str, list[SigiloNode]] = defaultdict(list)
    for node in nodes:
        grouped[node.schema].append(node)
    return grouped


def _schema_priority(name: str) -> tuple[int, str]:
    normalized = name.lower()
    priority = {
        "core": 0,
        "public": 0,
        "dbo": 0,
        "main": 0,
        "ledger": 1,
        "risk": 2,
        "reporting": 3,
        "analytics": 3,
    }.get(normalized, 10)
    return priority, normalized


def _compute_schema_levels(
    grouped: dict[str, list[SigiloNode]],
    config: SigiloConfig,
) -> list[_SchemaLevel]:
    if not grouped:
        return []

    levels: list[_SchemaLevel] = []
    for schema_name in sorted(grouped, key=_schema_priority):
        schema_nodes = grouped[schema_name]
        max_node_r = max(_node_radius(config, node) for node in schema_nodes)
        circumference_need = sum(_node_radius(config, node) * 2.5 + 18.0 for node in schema_nodes)
        orbit_r = max(
            config.schema_ring_r_base * 0.48,
            max_node_r + 28.0,
            circumference_need / (2.0 * math.pi),
        )
        support_r = orbit_r + max_node_r + 64.0
        levels.append(
            _SchemaLevel(name=schema_name, orbit_r=orbit_r, support_r=support_r, nodes=schema_nodes)
        )
    return levels


def _expand_canvas_for_layout(
    nodes: list[SigiloNode],
    config: SigiloConfig,
    width: float,
    height: float,
) -> tuple[float, float]:
    grouped = _group_nodes_by_schema(nodes)
    levels = _compute_schema_levels(grouped, config)
    if not levels:
        return width, height
    if not config.emit_macro_rings:
        return width, height
    max_support = max(level.support_r for level in levels)
    cluster_orbit = _schema_outer_orbit(levels, config, width, height)
    required_half = cluster_orbit + max_support + _SCHEMA_CANVAS_MARGIN + _SCHEMA_LABEL_DEPTH
    side = max(width, height, required_half * 2.0)
    return side, side


def _derive_schema_positions(
    placed_nodes: list[_PlacedNode],
    config: SigiloConfig,
) -> list[_PlacedSchema]:
    grouped: dict[str, list[_PlacedNode]] = defaultdict(list)
    for placed in placed_nodes:
        grouped[placed.node.schema].append(placed)

    out: list[_PlacedSchema] = []
    for schema_name in sorted(grouped, key=_schema_priority):
        schema_nodes = grouped[schema_name]
        cx = sum(node.cx for node in schema_nodes) / len(schema_nodes)
        cy = sum(node.cy for node in schema_nodes) / len(schema_nodes)
        spread = max(
            max(math.hypot(node.cx - cx, node.cy - cy) + node.r for node in schema_nodes),
            config.schema_ring_r_base * 0.72,
        )
        out.append(
            _PlacedSchema(
                name=schema_name,
                cx=cx,
                cy=cy,
                support_r=spread + 56.0,
                table_count=len(schema_nodes),
                direct_fk_count=sum(node.node.fk_count for node in schema_nodes),
                total_rows=sum(max(0, node.node.row_estimate or 0) for node in schema_nodes),
            )
        )
    return out


def _place_nodes_force(
    nodes: list[SigiloNode],
    config: SigiloConfig,
    width: float,
    height: float,
    force_params: dict[str, Any],
) -> list[_PlacedNode]:
    placed, _ = _place_nodes_circular(nodes, config, width, height)
    if len(placed) <= 5:
        return placed

    node_positions = [[item.cx, item.cy] for item in placed]
    schema_by_index = [item.node.schema for item in placed]
    # Keep the fallback deterministic but lightweight: reuse the circular seed and
    # only add a mild schema-aware relaxation without depending on the native library.
    iterations = int(force_params.get("force_iterations", 120))
    temperature = float(force_params.get("force_temperature", 1.0)) * min(width, height) * 0.35
    cooling = float(force_params.get("force_cooling", 0.98))
    area = width * height
    k = math.sqrt(area / max(len(placed), 1))

    for _ in range(max(1, min(iterations, 300))):
        disp = [[0.0, 0.0] for _ in placed]
        for i in range(len(placed)):
            for j in range(i + 1, len(placed)):
                dx = node_positions[i][0] - node_positions[j][0]
                dy = node_positions[i][1] - node_positions[j][1]
                dist = math.hypot(dx, dy) or 0.1
                ux = dx / dist
                uy = dy / dist
                rep = (k * k) / dist
                disp[i][0] += ux * rep
                disp[i][1] += uy * rep
                disp[j][0] -= ux * rep
                disp[j][1] -= uy * rep
                if schema_by_index[i] == schema_by_index[j]:
                    coh = 0.08 * dist / max(k, 1.0)
                    disp[i][0] -= ux * coh
                    disp[i][1] -= uy * coh
                    disp[j][0] += ux * coh
                    disp[j][1] += uy * coh
        for i in range(len(placed)):
            cx = width / 2.0 - node_positions[i][0]
            cy = height / 2.0 - node_positions[i][1]
            disp[i][0] += cx * 0.01
            disp[i][1] += cy * 0.01
        step_limit = max(1.0, temperature)
        for i in range(len(placed)):
            dx, dy = disp[i]
            dist = math.hypot(dx, dy)
            if dist > 0:
                limited = min(step_limit, dist)
                node_positions[i][0] += dx / dist * limited
                node_positions[i][1] += dy / dist * limited
            margin = max(30.0, placed[i].r + 14.0)
            node_positions[i][0] = min(width - margin, max(margin, node_positions[i][0]))
            node_positions[i][1] = min(height - margin, max(margin, node_positions[i][1]))
        temperature *= cooling

    return [
        _PlacedNode(node=item.node, cx=node_positions[index][0], cy=node_positions[index][1], r=item.r)
        for index, item in enumerate(placed)
    ]


def _stylesheet(config: SigiloConfig) -> str:
    dash = config.ring_stroke_dash or "none"
    font_label = _scale_font_spec(config.font_label)
    font_hash = _scale_font_spec(config.font_hash)
    db_core_label = _scale_font_spec("bold 11px monospace")
    db_core_sub = _scale_font_spec("8px monospace")
    schema_core_label = _scale_font_spec("8px monospace")
    schema_core_sub = _scale_font_spec("7px monospace")
    label_font = _scale_font_spec("8px monospace")
    col_label_font = _scale_font_spec("6px monospace")
    node_metric_font = _scale_font_spec("7px monospace")
    return f"""  <defs>
    <style><![CDATA[
      .bg {{ fill: #f7f1e5; }}
      .macro-ring {{ fill: none; stroke: #1f2937; stroke-width: 2.0; opacity: {config.ring_opacity:.2f}; stroke-dasharray: {dash}; }}
      .foundation-ring {{ fill: none; stroke: #1f2937; stroke-width: 1.3; opacity: 0.18; }}
      .db-core-shell {{ fill: rgba(255,252,246,0.86); stroke: #5a4b3f; stroke-width: 1.0; }}
      .db-core {{ fill: #d1fae5; stroke: #0f766e; stroke-width: 1.8; }}
      .db-core-mark {{ fill: none; stroke: #0f766e; stroke-width: 0.9; opacity: 0.75; }}
      .db-core-label {{ fill: #0f172a; font: {db_core_label}; }}
      .db-core-sub {{ fill: #475569; font: {db_core_sub}; opacity: 0.82; }}
      .schema-label {{ fill: #0f172a; font: {font_label}; opacity: 0.75; }}
      .schema-core-shell {{ fill: rgba(255,252,246,0.82); stroke: #5a4b3f; stroke-width: 0.92; }}
      .schema-core {{ fill: #f7efe3; stroke: #57483b; stroke-width: 1.0; }}
      .schema-core-mark {{ fill: none; stroke: #8b7763; stroke-width: 0.8; opacity: 0.72; }}
      .schema-core-label {{ fill: #0f172a; font: {schema_core_label}; opacity: 0.88; }}
      .schema-core-sub {{ fill: #475569; font: {schema_core_sub}; opacity: 0.72; }}
      .call {{ stroke: #4b5563; stroke-width: 1.5; fill: none; stroke-linecap: round; opacity: 0.82; }}
      .branch {{ stroke: #6b7280; stroke-width: 1.2; fill: none; stroke-dasharray: 5 4; opacity: 0.8; }}
      .node-main {{ fill: #f8fafc; stroke: #1f2937; stroke-width: 1.65; }}
      .node-aux  {{ fill: #f1f5f9; stroke: #475569; stroke-width: 1.15; stroke-dasharray: 4 3; }}
      .node-loop {{ fill: #ecfeff; stroke: #0f766e; stroke-width: 1.2; }}
      .node-fk   {{ fill: #fef9c3; stroke: #92400e; stroke-width: 1.1; stroke-dasharray: 6 3; }}
      .node-loop-inner {{ fill: none; stroke: #0f766e; stroke-width: 0.9; opacity: 0.8; }}
      .node-shell {{ fill: none; stroke: #5a4b3f; stroke-width: 1.0; opacity: 0.78; }}
      .node-foundation {{ fill: none; stroke: #b7a690; stroke-width: 0.8; opacity: 0.62; }}
      .col-orbit {{ fill: none; stroke: #c5b59f; stroke-width: 0.8; opacity: 0.60; }}
      .col-chord {{ fill: none; stroke: #7b6a59; stroke-width: 0.72; opacity: 0.52; }}
      .node-core {{ fill: #f7efe3; stroke: #57483b; stroke-width: 0.82; }}
      .col-spoke {{ stroke: #8d7a68; stroke-width: 0.82; opacity: 0.58; }}
      .col-pk {{ fill: #111827; }}
      .col-fk {{ fill: #0f766e; }}
      .col-reg {{ fill: #64748b; }}
      .col-nullable {{ opacity: 0.55; }}
      .hash {{ fill: #111827; font: {font_hash}; letter-spacing: 0.5px; }}
      .label {{ fill: #0f172a; font: {label_font}; opacity: 0.68; }}
      .col-label {{ fill: #0f172a; font: {col_label_font}; opacity: 0.78; }}
      .node-metric {{ fill: #334155; font: {node_metric_font}; opacity: 0.78; }}
      .system-node-wrap, .system-edge-wrap, .system-schema-wrap, .system-column-wrap, .system-column-link-wrap {{ cursor: help; }}
      .system-node-wrap:hover > circle {{ opacity: 0.94; }}
      .system-edge-wrap:hover > line, .system-edge-wrap:hover > path {{ opacity: 1.0; stroke-width: 2.2; }}
    ]]></style>
  </defs>"""


def _emit_database_foundation(width: float, height: float, config: SigiloConfig) -> str:
    cx = width / 2.0
    cy = height / 2.0
    max_r = min(width, height) * 0.47
    rings = [max_r * 0.22, max_r * 0.42, max_r * 0.68, max_r]
    core_r = max(28.0, min(width, height) * 0.055)
    parts = ['  <g id="database_foundation">']
    for radius in rings:
        parts.append(f'    <circle class="foundation-ring" cx="{cx:.1f}" cy="{cy:.1f}" r="{radius:.1f}"/>')
    parts.append(f'    <circle class="db-core-shell" cx="{cx:.1f}" cy="{cy:.1f}" r="{core_r + 8.0:.1f}"/>')
    parts.append(f'    <circle class="db-core" cx="{cx:.1f}" cy="{cy:.1f}" r="{core_r:.1f}"/>')
    parts.append(f'    <circle class="db-core-mark" cx="{cx:.1f}" cy="{cy:.1f}" r="{max(core_r - 8.0, 8.0):.1f}"/>')
    parts.append(
        f'    <text class="db-core-label" x="{cx:.1f}" y="{cy - 2.0:.1f}" text-anchor="middle">'
        f"{escape(config.core_label)}</text>"
    )
    parts.append(
        f'    <text class="db-core-sub" x="{cx:.1f}" y="{cy + 12.0:.1f}" text-anchor="middle">'
        f"{escape(config.core_subtitle)}</text>"
    )
    parts.append("  </g>")
    return "\n".join(parts)


def _emit_schema_labels(
    placed_schemas: list[_PlacedSchema],
    config: SigiloConfig,
) -> str:
    if not placed_schemas or not config.emit_macro_rings:
        return ""
    parts = ["  <g id=\"schema_rings\">"]
    for schema in placed_schemas:
        core_r = max(14.0, min(22.0, schema.support_r * 0.16))
        label_y = schema.cy + schema.support_r + 18.0
        attrs = ['class="system-schema-wrap"']
        if config.emit_data_attrs:
            attrs.extend(
                [
                    f'data-schema="{escape(schema.name, quote=True)}"',
                    f'data-table-count="{schema.table_count}"',
                    f'data-direct-fk-count="{schema.direct_fk_count}"',
                    f'data-total-rows="{schema.total_rows}"',
                ]
            )
        parts.append(f"    <g {' '.join(attrs)}>")
        parts.append(
            f'      <circle class="macro-ring" cx="{schema.cx:.1f}" cy="{schema.cy:.1f}" r="{schema.support_r:.1f}"/>'
        )
        parts.append(
            f'      <circle class="schema-core-shell" cx="{schema.cx:.1f}" cy="{schema.cy:.1f}" r="{core_r + 7.0:.1f}"/>'
        )
        parts.append(
            f'      <circle class="schema-core" cx="{schema.cx:.1f}" cy="{schema.cy:.1f}" r="{core_r:.1f}"/>'
        )
        parts.append(
            f'      <circle class="schema-core-mark" cx="{schema.cx:.1f}" cy="{schema.cy:.1f}" r="{max(core_r - 6.0, 6.0):.1f}"/>'
        )
        parts.append(
            f'      <text class="schema-core-label" x="{schema.cx:.1f}" y="{schema.cy + 3.0:.1f}" '
            f'text-anchor="middle">{escape(schema.name)}</text>'
        )
        parts.append(
            f'      <text class="schema-label" x="{schema.cx:.1f}" y="{label_y:.1f}" '
            f'text-anchor="middle">{escape(schema.name)}</text>'
        )
        parts.append(
            f'      <text class="schema-core-sub" x="{schema.cx:.1f}" y="{label_y + 12.0:.1f}" '
            f'text-anchor="middle">{schema.table_count} tables</text>'
        )
        if config.emit_titles:
            parts.append("      <title>" + escape(
                f"{schema.name}\ntables: {schema.table_count}\ndirect_fks: {schema.direct_fk_count}\nrows: ~{_fmt_number(schema.total_rows)}"
            ) + "</title>")
        parts.append("    </g>")
    parts.append("  </g>")
    return "\n".join(parts)


def _emit_edges(
    edges: list[SigiloEdge],
    node_map: dict[str, _PlacedNode],
    config: SigiloConfig,
) -> str:
    parts = ['  <g id="fk_edges">']
    for edge in edges:
        source = node_map.get(edge.from_id)
        target = node_map.get(edge.to_id)
        if source is None or target is None or source.node.id == target.node.id:
            continue
        css_class = "call" if edge.edge_type == "declared" else "branch"
        attrs: list[str] = ['class="system-edge-wrap"']
        if config.emit_data_attrs:
            attrs.extend(
                [
                    f'data-fk-from="{escape(edge.from_id, quote=True)}"',
                    f'data-fk-to="{escape(edge.to_id, quote=True)}"',
                    f'data-fk-type="{escape(edge.edge_type, quote=True)}"',
                    (
                        'data-relationship-kind="direct"'
                        if edge.edge_type == "declared"
                        else 'data-relationship-kind="indirect"'
                    ),
                ]
            )
            if edge.from_column and edge.to_column:
                attrs.append(
                    f'data-fk-columns="{escape(edge.from_column, quote=True)}-&gt;'
                    f'{escape(edge.to_column, quote=True)}"'
                )
            if edge.on_delete:
                attrs.append(f'data-on-delete="{escape(edge.on_delete, quote=True)}"')
        parts.append(f"    <g {' '.join(attrs)}>")
        parts.append(
            f'      <line class="{css_class}" x1="{source.cx:.2f}" y1="{source.cy:.2f}" '
            f'x2="{target.cx:.2f}" y2="{target.cy:.2f}"/>'
        )
        if config.emit_titles:
            title = (
                f"FK: {edge.from_id}"
                f"{'.' + edge.from_column if edge.from_column else ''} -> "
                f"{edge.to_id}{'.' + edge.to_column if edge.to_column else ''}\n"
                f"type: {edge.edge_type}"
            )
            if edge.on_delete:
                title += f" | ON DELETE {edge.on_delete}"
            parts.append(f"      <title>{escape(title)}</title>")
        parts.append("    </g>")
    parts.append("  </g>")
    return "\n".join(parts)


def _emit_nodes(placed_nodes: list[_PlacedNode], config: SigiloConfig) -> str:
    parts = ['  <g id="table_nodes">']
    for placed in placed_nodes:
        node = placed.node
        attrs: list[str] = ['class="system-node-wrap"']
        if config.emit_data_attrs:
            columns_detail = _serialize_columns_detail(node)
            attrs.extend(
                [
                    f'data-table="{escape(node.name, quote=True)}"',
                    f'data-schema="{escape(node.schema, quote=True)}"',
                    f'data-row-estimate="{node.row_estimate if node.row_estimate is not None else -1}"',
                    f'data-size-bytes="{node.size_bytes if node.size_bytes is not None else -1}"',
                    f'data-column-count="{len(node.columns)}"',
                    f'data-fk-count="{node.fk_count}"',
                    f'data-index-count="{node.index_count}"',
                    f'data-table-type="{escape(node.node_type, quote=True)}"',
                    f'data-columns-detail="{escape(columns_detail, quote=True)}"',
                ]
            )
            if node.comment:
                attrs.append(f'data-comment="{escape(node.comment, quote=True)}"')
        parts.append(f"    <g {' '.join(attrs)}>")
        parts.append(
            f'      <circle class="{_node_class(node.node_type)}" cx="{placed.cx:.2f}" '
            f'cy="{placed.cy:.2f}" r="{placed.r:.2f}"/>'
        )
        if node.node_type == "materialized_view":
            parts.append(
                f'      <circle class="node-loop-inner" cx="{placed.cx:.2f}" cy="{placed.cy:.2f}" '
                f'r="{max(placed.r - 4.0, 2.0):.2f}"/>'
            )
        if config.emit_titles:
            title = (
                f"{node.name}\nschema: {node.schema}\nrows: ~{_fmt_number(node.row_estimate)}\n"
                f"size: {_fmt_bytes(node.size_bytes)}\ncolumns: {len(node.columns)}"
            )
            if node.comment:
                title += f"\n{node.comment}"
            if node.columns:
                preview = "\n".join(
                    f"- {column.name}: {column.type_str or 'unknown'}{_column_flag_suffix(column)}"
                    for column in node.columns[:10]
                )
                title += f"\n\n{preview}"
                if len(node.columns) > 10:
                    title += f"\n+{len(node.columns) - 10} more columns"
            parts.append(f"      <title>{escape(title)}</title>")
        parts.append("    </g>")
        parts.extend(_emit_table_internals(placed))
        if config.emit_column_dots:
            parts.extend(_emit_column_sigils(placed, config))
        parts.append(
            f'    <text class="hash" x="{placed.cx:.1f}" y="{placed.cy + placed.r + 14.0:.1f}" '
            f'text-anchor="middle">{escape(node.name)}</text>'
        )
    parts.append("  </g>")
    return "\n".join(parts)


def _emit_table_internals(placed: _PlacedNode) -> list[str]:
    shell_r = max(placed.r - 6.0, 12.0)
    inner_r = max(placed.r * 0.76, 12.0)
    core_r = max(placed.r * 0.16, 6.0)
    return [
        f'      <circle class="node-shell" cx="{placed.cx:.2f}" cy="{placed.cy:.2f}" r="{shell_r:.2f}"/>',
        f'      <ellipse class="node-foundation" cx="{placed.cx:.2f}" cy="{placed.cy:.2f}" rx="{inner_r:.2f}" ry="{max(inner_r * 0.74, 8.0):.2f}"/>',
        f'      <circle class="node-core" cx="{placed.cx:.2f}" cy="{placed.cy:.2f}" r="{core_r:.2f}"/>',
    ]


def _emit_column_sigils(placed: _PlacedNode, config: SigiloConfig) -> list[str]:
    columns = placed.node.columns
    if not columns:
        return []
    lines: list[str] = []
    for band_radius in _column_band_radii(placed.r, len(columns)):
        lines.append(
            f'      <circle class="col-orbit" cx="{placed.cx:.2f}" cy="{placed.cy:.2f}" r="{band_radius:.2f}"/>'
        )

    layout = _column_layout(placed, columns)
    for left, right in _band_links(layout):
        left_col, x1, y1, _, _, _ = left
        right_col, x2, y2, _, _, _ = right
        ctrl_x = (placed.cx + x2) / 2.0
        ctrl_y = (placed.cy + y2) / 2.0
        link_title = f"{placed.node.schema}.{placed.node.name}\n{left_col.name} ↔ {right_col.name}"
        link_attrs = ['class="system-column-link-wrap"']
        if config.emit_data_attrs:
            link_attrs.extend(
                [
                    f'data-table="{escape(placed.node.name, quote=True)}"',
                    f'data-schema="{escape(placed.node.schema, quote=True)}"',
                    f'data-column-left="{escape(left_col.name, quote=True)}"',
                    f'data-column-right="{escape(right_col.name, quote=True)}"',
                ]
            )
        lines.append(f"    <g {' '.join(link_attrs)}>")
        lines.append(
            f'      <path class="col-chord" d="M {x1:.2f} {y1:.2f} Q {ctrl_x:.2f} {ctrl_y:.2f} {x2:.2f} {y2:.2f}"/>'
        )
        if config.emit_titles:
            lines.append(f'      <title>{escape(link_title)}</title>')
        lines.append("    </g>")

    for column, x, y, label_x, label_y, band in layout:
        column_type = column.type_str or "unknown"
        column_title = (
            f"{placed.node.schema}.{placed.node.name}\n"
            f"{column.name}: {column_type}\n"
            f"{_column_flags_string(column)}"
        )
        column_attrs = ['class="system-column-wrap"']
        if config.emit_data_attrs:
            column_attrs.extend(
                [
                    f'data-table="{escape(placed.node.name, quote=True)}"',
                    f'data-schema="{escape(placed.node.schema, quote=True)}"',
                    f'data-column-name="{escape(column.name, quote=True)}"',
                    f'data-column-type="{escape(column_type, quote=True)}"',
                    f'data-column-flags="{escape(_column_flags_string(column), quote=True)}"',
                    f'data-column-role="{escape(_column_role(column), quote=True)}"',
                ]
            )
        lines.append(f"    <g {' '.join(column_attrs)}>")
        lines.append(
            f'      <line class="col-spoke" x1="{placed.cx:.2f}" y1="{placed.cy:.2f}" '
            f'x2="{x:.2f}" y2="{y:.2f}"/>'
        )
        dot_class = "col-pk" if column.is_pk else "col-fk" if column.is_fk else "col-reg"
        nullable_class = " col-nullable" if column.is_nullable and not column.is_pk else ""
        lines.append(
            f'      <circle class="{dot_class}{nullable_class}" cx="{x:.2f}" '
            f'cy="{y:.2f}" r="{_COL_DOT_RADIUS:.1f}"/>'
        )
        should_label = band == 0 and (column.is_pk or column.is_fk)
        if should_label:
            lines.append(
                f'      <text class="col-label" x="{label_x:.2f}" y="{label_y:.2f}" '
                f'text-anchor="middle">{escape(_short_column_label(column.name))}</text>'
            )
        if config.emit_titles:
            lines.append(f'      <title>{escape(column_title)}</title>')
        lines.append("    </g>")
    return lines


def _emit_table_metrics(placed: _PlacedNode) -> list[str]:
    node = placed.node
    rows = _fmt_number(node.row_estimate)
    metrics = f"{len(node.columns)} cols · {node.fk_count} fk · {node.index_count} idx"
    return [
        f'      <text class="node-metric" x="{placed.cx:.1f}" y="{placed.cy + placed.r * 0.50:.1f}" '
        f'text-anchor="middle">{escape(metrics)}</text>',
        f'      <text class="node-metric" x="{placed.cx:.1f}" y="{placed.cy + placed.r * 0.64:.1f}" '
        f'text-anchor="middle">~{escape(rows)} rows</text>',
    ]


def _column_band_radii(node_radius: float, column_count: int) -> list[float]:
    bands = max(1, min(3, math.ceil(column_count / _COLUMN_BAND_SIZE)))
    start = max(node_radius * 0.36, 16.0)
    step = max(10.0, node_radius * 0.13)
    return [start + index * step for index in range(bands)]


def _column_layout(
    placed: _PlacedNode,
    columns: list[SigiloColumnDesc],
) -> list[tuple[SigiloColumnDesc, float, float, float, float, int]]:
    entries: list[tuple[SigiloColumnDesc, float, float, float, float, int]] = []
    band_radii = _column_band_radii(placed.r, len(columns))
    for index, column in enumerate(columns):
        band = min(len(band_radii) - 1, index // _COLUMN_BAND_SIZE)
        within_band = index % _COLUMN_BAND_SIZE
        total_in_band = min(_COLUMN_BAND_SIZE, len(columns) - band * _COLUMN_BAND_SIZE)
        angle = 2.0 * math.pi * within_band / max(total_in_band, 1) - math.pi / 2.0
        radius = band_radii[band]
        x = placed.cx + radius * math.cos(angle)
        y = placed.cy + radius * math.sin(angle)
        label_radius = max(radius - 12.0, placed.r * 0.18)
        label_x = placed.cx + label_radius * math.cos(angle)
        label_y = placed.cy + label_radius * math.sin(angle) + 2.0
        entries.append((column, x, y, label_x, label_y, band))
    return entries


def _band_links(
    layout: list[tuple[SigiloColumnDesc, float, float, float, float, int]],
) -> list[tuple[
    tuple[SigiloColumnDesc, float, float, float, float, int],
    tuple[SigiloColumnDesc, float, float, float, float, int],
]]:
    grouped: dict[int, list[tuple[SigiloColumnDesc, float, float, float, float, int]]] = defaultdict(list)
    for entry in layout:
        grouped[entry[5]].append(entry)
    links: list[tuple[
        tuple[SigiloColumnDesc, float, float, float, float, int],
        tuple[SigiloColumnDesc, float, float, float, float, int],
    ]] = []
    for entries in grouped.values():
        if len(entries) < 2:
            continue
        for index, entry in enumerate(entries):
            links.append((entry, entries[(index + 1) % len(entries)]))
    return links


def _short_column_label(name: str) -> str:
    return name if len(name) <= 12 else f"{name[:9]}..."


def _column_flag_suffix(column: Any) -> str:
    flags: list[str] = []
    if getattr(column, "is_pk", False):
        flags.append("PK")
    if getattr(column, "is_fk", False):
        flags.append("FK")
    flags.append("NULL" if getattr(column, "is_nullable", True) else "NOT NULL")
    if getattr(column, "distinct_estimate", None) is not None:
        flags.append(f"distinct={column.distinct_estimate}")
    if getattr(column, "null_rate", None) is not None:
        flags.append(f"null={column.null_rate:.2f}")
    return f" [{' | '.join(flags)}]" if flags else ""


def _column_flags_string(column: SigiloColumnDesc) -> str:
    flags: list[str] = []
    if column.is_pk:
        flags.append("PK")
    if column.is_fk:
        flags.append("FK")
    flags.append("NULL" if column.is_nullable else "NOT NULL")
    if column.distinct_estimate is not None:
        flags.append(f"distinct={column.distinct_estimate}")
    if column.null_rate is not None:
        flags.append(f"null={column.null_rate:.2f}")
    return " | ".join(flags)


def _column_role(column: SigiloColumnDesc) -> str:
    if column.is_pk:
        return "primary_key"
    if column.is_fk:
        return "foreign_key"
    return "column"


def _serialize_columns_detail(node: SigiloNode) -> str:
    parts: list[str] = []
    for column in node.columns:
        flags: list[str] = []
        if column.is_pk:
            flags.append("PK")
        if column.is_fk:
            flags.append("FK")
        flags.append("NULL" if column.is_nullable else "NOT NULL")
        if column.distinct_estimate is not None:
            flags.append(f"distinct={column.distinct_estimate}")
        if column.null_rate is not None:
            flags.append(f"null={column.null_rate:.2f}")
        parts.append(
            "::".join(
                [
                    column.name,
                    column.type_str or "unknown",
                    ",".join(flags),
                ]
            )
        )
    return "||".join(parts)


def _node_class(node_type: str) -> str:
    return {
        "table": "node-main",
        "view": "node-aux",
        "materialized_view": "node-loop",
        "foreign_table": "node-fk",
    }.get(node_type, "node-main")


def _fmt_number(value: int | None) -> str:
    if value is None or value < 0:
        return "unknown"
    return f"{value:,}".replace(",", " ")


def _fmt_bytes(value: int | None) -> str:
    if value is None or value < 0:
        return "unknown"
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024.0:.1f} KB"
    if value < 1024 * 1024 * 1024:
        return f"{value / (1024.0 * 1024):.1f} MB"
    return f"{value / (1024.0 * 1024 * 1024):.2f} GB"
