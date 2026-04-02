"""High-level sigilo builder backed by the native renderer when available."""

from __future__ import annotations

import logging
import math
import re
from html import escape, unescape
from typing import Any, Literal

import atlas._sigilo as _sigilo
from atlas.sigilo import _python_fallback
from atlas.sigilo.hover import HoverScriptBuilder
from atlas.sigilo.types import SigiloColumnDesc, SigiloConfig, SigiloEdge, SigiloNode
from atlas.types import ColumnInfo, IntrospectionResult, TableInfo, TableType

logger = logging.getLogger(__name__)

_NODE_TAG_RE = re.compile(r"<g class=\"system-node-wrap\"[^>]*>")
_COLUMN_TAG_RE = re.compile(r"<g class=\"system-column-wrap\"[^>]*>")
_ATTR_RE = re.compile(r"([:\w.-]+)=\"([^\"]*)\"")

_NODE_TYPE_BY_TABLE_TYPE: dict[TableType, str] = {
    TableType.TABLE: "table",
    TableType.VIEW: "view",
    TableType.MATERIALIZED_VIEW: "materialized_view",
    TableType.FOREIGN_TABLE: "foreign_table",
    TableType.SYNONYM: "view",
}


def compute_visual_node_radius(
    row_estimate: int,
    column_count: int,
    *,
    r_min: float,
    r_max: float,
    r_ref_rows: int,
) -> float:
    """Balance row volume and column detail so table nodes remain visually useful."""

    effective_rows = max(0, row_estimate)
    if effective_rows <= 0:
        row_factor = 0.0
    else:
        row_factor = math.log1p(float(effective_rows)) / math.log1p(float(max(1, r_ref_rows)))
    row_radius = r_min + (r_max - r_min) * min(1.0, max(0.0, row_factor))

    effective_columns = max(0, column_count)
    if effective_columns == 0:
        detail_radius = r_min
    else:
        bands = max(1, min(3, math.ceil(effective_columns / 12.0)))
        density_factor = min(1.0, math.sqrt(float(effective_columns)) / math.sqrt(36.0))
        detail_factor = min(0.74, 0.14 + bands * 0.14 + density_factor * 0.22)
        detail_radius = r_min + (r_max - r_min) * detail_factor
    return max(r_min, min(r_max, max(row_radius, detail_radius)))


class SigiloBuilder:
    """Convert Atlas introspection metadata into an SVG sigilo."""

    def __init__(
        self,
        result: IntrospectionResult,
        config: SigiloConfig | None = None,
        *,
        prefer_native: bool = True,
    ) -> None:
        self.result = result
        self.config = config or SigiloConfig()
        self.prefer_native = prefer_native

    def build_nodes(self) -> list[SigiloNode]:
        """Convert every table-like object into a renderable sigilo node."""
        return [self._table_to_node(table) for schema in self.result.schemas for table in schema.tables]

    def build_edges(self) -> list[SigiloEdge]:
        """Convert every foreign key into a sigilo edge."""
        edges: list[SigiloEdge] = []
        for schema in self.result.schemas:
            for table in schema.tables:
                for foreign_key in table.foreign_keys:
                    edges.append(
                        SigiloEdge(
                            from_id=f"{foreign_key.source_schema}.{foreign_key.source_table}",
                            to_id=f"{foreign_key.target_schema}.{foreign_key.target_table}",
                            from_column=", ".join(foreign_key.source_columns) or None,
                            to_column=", ".join(foreign_key.target_columns) or None,
                            on_delete=foreign_key.on_delete or None,
                            edge_type="inferred" if foreign_key.is_inferred else "declared",
                        )
                    )
        return edges

    def build_svg(self) -> bytes:
        """Render the current introspection result into SVG bytes."""
        return self._render_from(self.build_nodes(), self.build_edges())

    def _render_from(
        self,
        nodes: list[SigiloNode],
        edges: list[SigiloEdge],
        layout: Literal["circular", "force"] = "circular",
        force_params: dict[str, Any] | None = None,
    ) -> bytes:
        """Render from pre-built nodes and edges supplied by a richer collector."""

        if self.prefer_native and _sigilo.available():
            try:
                svg = self._render_via_c_from(nodes, edges, layout, force_params or {})
                return self._inject_hover_script(self._inject_semantic_attrs(svg))
            except Exception as exc:  # pragma: no cover - exercised via monkeypatch tests
                logger.warning("Native sigilo renderer failed, falling back to Python: %s", exc)
        return self._inject_hover_script(
            self._inject_semantic_attrs(
                self._python_fallback_from(nodes, edges, layout, force_params or {})
            )
        )

    def _build_svg_native(self, nodes: list[SigiloNode], edges: list[SigiloEdge]) -> bytes:
        return self._render_via_c_from(nodes, edges, "circular", {})

    def _render_via_c_from(
        self,
        nodes: list[SigiloNode],
        edges: list[SigiloEdge],
        layout: Literal["circular", "force"],
        force_params: dict[str, Any],
    ) -> bytes:
        with _sigilo.RenderContext(self.config) as ctx:
            for node in nodes:
                idx = ctx.add_node(node)
                if idx < 0:
                    raise RuntimeError(f"atlas_render_add_node failed: {ctx.error_message}")
            for edge in edges:
                if not ctx.add_edge(edge):
                    raise RuntimeError(f"atlas_render_add_edge failed: {ctx.error_message}")
            if layout == "force" and len(nodes) > 5 and hasattr(ctx, "compute_layout_force"):
                ctx.compute_layout_force(
                    iterations=int(force_params.get("force_iterations", 300)),
                    temperature=float(force_params.get("force_temperature", 1.0)),
                    cooling=float(force_params.get("force_cooling", 0.98)),
                )
            elif hasattr(ctx, "compute_layout"):
                ctx.compute_layout()
            try:
                return ctx.render(compute_layout=False)
            except TypeError:
                return ctx.render()

    def _python_fallback_from(
        self,
        nodes: list[SigiloNode],
        edges: list[SigiloEdge],
        layout: Literal["circular", "force"],
        force_params: dict[str, Any],
    ) -> bytes:
        try:
            return _python_fallback.render_svg(nodes, edges, self.config, layout, force_params)
        except TypeError:
            return _python_fallback.render_svg(nodes, edges, self.config)

    def _inject_hover_script(self, svg_bytes: bytes) -> bytes:
        closing = b"</svg>"
        if closing not in svg_bytes:
            return svg_bytes
        script = HoverScriptBuilder().build_script().encode("utf-8")
        return svg_bytes.replace(closing, script + closing, 1)

    def _inject_semantic_attrs(self, svg_bytes: bytes) -> bytes:
        text = svg_bytes.decode("utf-8")
        table_map = {
            (table.schema, table.name): self._table_semantic_attrs(table)
            for table in self.result.all_tables()
        }
        column_map = {
            (table.schema, table.name, column.name): self._column_semantic_attrs(column)
            for table in self.result.all_tables()
            for column in table.columns
        }

        def _replace_node(match: re.Match[str]) -> str:
            tag = match.group(0)
            attrs = dict(_ATTR_RE.findall(tag))
            schema = unescape(attrs.get("data-schema", ""))
            table_name = unescape(attrs.get("data-table", ""))
            semantic_attrs = table_map.get((schema, table_name), {})
            return self._merge_data_attrs(tag, semantic_attrs)

        def _replace_column(match: re.Match[str]) -> str:
            tag = match.group(0)
            attrs = dict(_ATTR_RE.findall(tag))
            schema = unescape(attrs.get("data-schema", ""))
            table_name = unescape(attrs.get("data-table", ""))
            column_name = unescape(attrs.get("data-column-name", ""))
            semantic_attrs = column_map.get((schema, table_name, column_name), {})
            return self._merge_data_attrs(tag, semantic_attrs)

        text = _NODE_TAG_RE.sub(_replace_node, text)
        text = _COLUMN_TAG_RE.sub(_replace_column, text)
        return text.encode("utf-8")

    @staticmethod
    def _merge_data_attrs(tag: str, semantic_attrs: dict[str, str]) -> str:
        if not semantic_attrs:
            return tag
        insertion = "".join(
            f' {name}="{escape(value, quote=True)}"'
            for name, value in semantic_attrs.items()
            if value != ""
        )
        if not insertion:
            return tag
        return f"{tag[:-1]}{insertion}>"

    @staticmethod
    def _table_semantic_attrs(table: TableInfo) -> dict[str, str]:
        attrs: dict[str, str] = {}
        if table.semantic_short:
            attrs["data-semantic-short"] = table.semantic_short
        if table.semantic_detailed:
            attrs["data-semantic-detailed"] = table.semantic_detailed
        if table.semantic_role:
            attrs["data-semantic-role"] = table.semantic_role
        if table.semantic_domain:
            attrs["data-semantic-domain"] = table.semantic_domain
        if (
            table.semantic_short
            or table.semantic_detailed
            or table.semantic_role
            or table.semantic_domain
            or table.semantic_confidence > 0.0
        ):
            attrs["data-semantic-confidence"] = f"{table.semantic_confidence:.3f}".rstrip("0").rstrip(".")
        return attrs

    @staticmethod
    def _column_semantic_attrs(column: ColumnInfo) -> dict[str, str]:
        attrs: dict[str, str] = {}
        if column.semantic_short:
            attrs["data-semantic-short"] = column.semantic_short
        if column.semantic_detailed:
            attrs["data-semantic-detailed"] = column.semantic_detailed
        if column.semantic_role:
            attrs["data-semantic-role"] = column.semantic_role
        if (
            column.semantic_short
            or column.semantic_detailed
            or column.semantic_role
            or column.semantic_confidence > 0.0
        ):
            attrs["data-semantic-confidence"] = (
                f"{column.semantic_confidence:.3f}".rstrip("0").rstrip(".")
            )
        return attrs

    def _table_to_node(self, table: TableInfo) -> SigiloNode:
        return SigiloNode(
            id=table.qualified_name,
            name=table.name,
            schema=table.schema,
            node_type=_NODE_TYPE_BY_TABLE_TYPE.get(table.table_type, "table"),
            comment=table.comment,
            row_estimate=table.row_count_estimate if table.row_count_estimate > 0 else None,
            size_bytes=table.size_bytes if table.size_bytes > 0 else None,
            columns=[self._column_to_desc(column) for column in table.columns],
            fk_count=len(table.foreign_keys),
            index_count=len(table.indexes),
            r=compute_visual_node_radius(
                table.row_count_estimate,
                len(table.columns),
                r_min=self.config.node_r_min,
                r_max=self.config.node_r_max,
                r_ref_rows=self.config.node_r_ref_rows,
            ),
        )

    def _column_to_desc(self, column: ColumnInfo) -> SigiloColumnDesc:
        distinct_estimate = column.stats.distinct_count if column.stats.distinct_count > 0 else None
        null_rate = (
            column.stats.null_rate
            if column.stats.row_count > 0 or column.stats.null_count > 0
            else None
        )
        return SigiloColumnDesc(
            name=column.name,
            type_str=column.native_type,
            is_pk=column.is_primary_key,
            is_fk=column.is_foreign_key,
            is_nullable=column.is_nullable,
            distinct_estimate=distinct_estimate,
            null_rate=null_rate,
        )
