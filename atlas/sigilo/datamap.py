"""Datamap-oriented sigilo builder for rich database discovery graphs."""

from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from typing import Literal

from atlas.sigilo.builder import SigiloBuilder, compute_visual_node_radius
from atlas.sigilo.style import SigiloStyle, StyleParams, get_style_params
from atlas.sigilo.types import SigiloColumnDesc, SigiloConfig, SigiloEdge, SigiloNode
from atlas.types import ColumnInfo, ForeignKeyInfo, IntrospectionResult, TableType

SizeScaleMode = Literal["log", "linear", "sqrt"]
LayoutMode = Literal["circular", "force"]


@dataclass(frozen=True, slots=True)
class _RowScaleProfile:
    """Per-database row-distribution profile used to size table nodes."""

    reference_rows: int
    minimum_rows: int
    mean_rows: float
    sorted_positive_rows: tuple[int, ...]

    @property
    def has_distribution(self) -> bool:
        return bool(self.sorted_positive_rows)


def _build_row_scale_profile(
    row_estimates: list[int],
    *,
    default_reference_rows: float,
) -> _RowScaleProfile:
    """Build a stable row profile from the currently rendered tables."""

    positive_rows = tuple(sorted(row for row in row_estimates if row > 0))
    if not positive_rows:
        return _RowScaleProfile(
            reference_rows=max(1, int(default_reference_rows)),
            minimum_rows=1,
            mean_rows=1.0,
            sorted_positive_rows=(),
        )
    return _RowScaleProfile(
        reference_rows=max(1, positive_rows[-1]),
        minimum_rows=max(1, positive_rows[0]),
        mean_rows=sum(positive_rows) / len(positive_rows),
        sorted_positive_rows=positive_rows,
    )


def _row_percentile_factor(row_estimate: int, profile: _RowScaleProfile) -> float:
    """Return the relative rank of a row estimate inside the current database."""

    if not profile.has_distribution:
        return 0.0
    if len(profile.sorted_positive_rows) == 1:
        return 1.0

    lower = bisect_left(profile.sorted_positive_rows, row_estimate)
    upper = bisect_right(profile.sorted_positive_rows, row_estimate)
    centered_rank = (lower + upper - 1) / 2.0
    return max(0.0, min(1.0, centered_rank / (len(profile.sorted_positive_rows) - 1)))


def _table_type_to_node_type(table_type: TableType) -> str:
    """Map Atlas table categories into sigilo node categories."""

    mapping = {
        TableType.TABLE: "table",
        TableType.VIEW: "view",
        TableType.MATERIALIZED_VIEW: "materialized_view",
        TableType.FOREIGN_TABLE: "foreign_table",
        TableType.SYNONYM: "view",
    }
    return mapping.get(table_type, "table")


def _column_to_desc(col: ColumnInfo) -> SigiloColumnDesc:
    """Convert canonical column metadata into the renderable sigilo subset."""

    distinct_estimate = col.stats.distinct_count if col.stats.distinct_count > 0 else None
    null_rate = col.stats.null_rate if col.stats.row_count > 0 or col.stats.null_count > 0 else None
    return SigiloColumnDesc(
        name=col.name,
        type_str=col.native_type or "",
        is_pk=bool(col.is_primary_key),
        is_fk=bool(col.is_foreign_key),
        is_nullable=bool(col.is_nullable),
        distinct_estimate=distinct_estimate,
        null_rate=null_rate,
    )


def _compute_node_radius(
    row_estimate: int,
    r_min: float,
    r_max: float,
    r_scale: float,
    scale_mode: SizeScaleMode,
    profile: _RowScaleProfile | None = None,
) -> float:
    """Compute a node radius from its row estimate using the selected scale mode."""

    rows = max(0, row_estimate)
    if r_scale <= 0:
        global_factor = 0.0
    else:
        reference_rows = max(1.0, float(profile.reference_rows) if profile is not None else r_scale)
        minimum_rows = max(1.0, float(profile.minimum_rows) if profile is not None else 1.0)
        if scale_mode == "linear":
            numerator = rows - minimum_rows
            denominator = max(1.0, reference_rows - minimum_rows)
            global_factor = numerator / denominator
        elif scale_mode == "sqrt":
            numerator = math.sqrt(max(rows, 0.0)) - math.sqrt(minimum_rows)
            denominator = max(1.0, math.sqrt(reference_rows) - math.sqrt(minimum_rows))
            global_factor = numerator / denominator
        else:
            numerator = math.log1p(rows) - math.log1p(minimum_rows)
            denominator = max(1e-9, math.log1p(reference_rows) - math.log1p(minimum_rows))
            global_factor = numerator / denominator
    global_factor = max(0.0, min(1.0, global_factor))

    if profile is None or not profile.has_distribution or len(profile.sorted_positive_rows) < 3:
        factor = global_factor
    else:
        mean_ratio = math.log((rows + 1.0) / (profile.mean_rows + 1.0))
        local_factor = 0.5 + 0.5 * math.tanh(mean_ratio * 2.35)
        percentile_factor = _row_percentile_factor(rows, profile)
        factor = 0.45 * global_factor + 0.35 * local_factor + 0.20 * percentile_factor
    factor = max(0.0, min(1.0, factor))
    return r_min + (r_max - r_min) * factor


def _make_node_id(schema: str, table: str) -> str:
    """Build a stable node id while normalizing dots and dashes inside identifiers."""

    schema_part = schema.replace(".", "_").replace("-", "_")
    table_part = table.replace(".", "_").replace("-", "_")
    return f"{schema_part}.{table_part}"


class DatamapSigiloBuilder:
    """Build a rich navigable sigilo SVG from Atlas introspection metadata."""

    def __init__(self, result: IntrospectionResult) -> None:
        self._result = result
        self._schema_filter: set[str] | None = None
        self._size_scale: SizeScaleMode = "log"
        self._style = SigiloStyle.NETWORK
        self._layout: LayoutMode = "circular"
        self._force_iterations = 300
        self._force_temperature = 1.0
        self._force_cooling = 0.98

    @classmethod
    def from_introspection_result(cls, result: IntrospectionResult) -> DatamapSigiloBuilder:
        """Create the builder from an introspection result."""

        return cls(result)

    def set_schema_filter(self, schemas: list[str] | None) -> DatamapSigiloBuilder:
        """Restrict the rendered sigilo to the provided schemas."""

        self._schema_filter = set(schemas) if schemas else None
        return self

    def set_size_scale(self, mode: SizeScaleMode = "log") -> DatamapSigiloBuilder:
        """Select the row-estimate to radius scale function."""

        if mode not in {"log", "linear", "sqrt"}:
            raise ValueError("mode must be one of 'log', 'linear', or 'sqrt'")
        self._size_scale = mode
        return self

    def set_style(
        self,
        style: Literal["network", "seal", "compact"] | SigiloStyle = "network",
    ) -> DatamapSigiloBuilder:
        """Select the visual style preset."""

        self._style = style if isinstance(style, SigiloStyle) else SigiloStyle.from_str(style)
        return self

    def set_layout(self, layout: LayoutMode = "circular") -> DatamapSigiloBuilder:
        """Select the node layout algorithm."""

        if layout not in {"circular", "force"}:
            raise ValueError("layout must be 'circular' or 'force'")
        self._layout = layout
        return self

    def set_force_params(
        self,
        iterations: int = 300,
        temperature: float = 1.0,
        cooling: float = 0.98,
    ) -> DatamapSigiloBuilder:
        """Configure future force-layout parameters."""

        if iterations < 1:
            raise ValueError("iterations must be >= 1")
        if temperature <= 0.0:
            raise ValueError("temperature must be > 0")
        if not 0.0 < cooling < 1.0:
            raise ValueError("cooling must be in the open interval (0, 1)")
        self._force_iterations = iterations
        self._force_temperature = temperature
        self._force_cooling = cooling
        return self

    def build(self) -> bytes:
        """Render the configured datamap sigilo to SVG bytes."""

        nodes, edges, schema_names = self._collect()
        if not nodes:
            raise RuntimeError(
                "IntrospectionResult does not contain renderable tables after applying filters."
            )

        params = get_style_params(self._style)
        config = self._build_config(params, schema_names)
        builder = SigiloBuilder(self._result, config=config)
        return builder._render_from(
            nodes,
            edges,
            self._layout,
            {
                "force_iterations": self._force_iterations,
                "force_temperature": self._force_temperature,
                "force_cooling": self._force_cooling,
            },
        )

    def rebuild_with_semantics(self, result: IntrospectionResult | None = None) -> bytes:
        """Re-render the current datamap after semantic enrichment."""

        if result is not None:
            self._result = result
        return self.build()

    def _collect(self) -> tuple[list[SigiloNode], list[SigiloEdge], list[str]]:
        """Collect the rich sigilo nodes, edges, and included schema names."""

        params = get_style_params(self._style)
        nodes: list[SigiloNode] = []
        edges: list[SigiloEdge] = []
        schema_names: list[str] = []
        node_ids: set[str] = set()
        included_tables = [
            table
            for schema in self._result.schemas
            if self._schema_filter is None or schema.name in self._schema_filter
            for table in schema.tables
        ]
        row_scale_profile = _build_row_scale_profile(
            [table.row_count_estimate for table in included_tables],
            default_reference_rows=params.node_r_scale,
        )

        for schema in self._result.schemas:
            if self._schema_filter is not None and schema.name not in self._schema_filter:
                continue
            schema_names.append(schema.name)
            for table in schema.tables:
                node_id = _make_node_id(schema.name, table.name)
                node_ids.add(node_id)
                nodes.append(
                    SigiloNode(
                        id=node_id,
                        name=table.name,
                        schema=schema.name,
                        comment=table.comment,
                        node_type=_table_type_to_node_type(table.table_type),
                        row_estimate=table.row_count_estimate if table.row_count_estimate > 0 else None,
                        size_bytes=table.size_bytes if table.size_bytes > 0 else None,
                        columns=[_column_to_desc(column) for column in table.columns],
                        fk_count=len(table.foreign_keys),
                        index_count=len(table.indexes),
                        r=_compute_node_radius(
                            row_estimate=table.row_count_estimate,
                            r_min=params.node_r_min * params.node_r_compact,
                            r_max=params.node_r_max * params.node_r_compact,
                            r_scale=params.node_r_scale,
                            scale_mode=self._size_scale,
                            profile=row_scale_profile,
                        ),
                    )
                )
                nodes[-1].r = max(
                    nodes[-1].r,
                    compute_visual_node_radius(
                        table.row_count_estimate,
                        len(table.columns),
                        r_min=params.node_r_min * params.node_r_compact,
                        r_max=params.node_r_max * params.node_r_compact,
                        r_ref_rows=int(params.node_r_scale),
                    ),
                )

        for schema in self._result.schemas:
            if self._schema_filter is not None and schema.name not in self._schema_filter:
                continue
            for table in schema.tables:
                source_id = _make_node_id(schema.name, table.name)
                for foreign_key in table.foreign_keys:
                    edge = self._build_edge(foreign_key)
                    if edge is None:
                        continue
                    if source_id != edge.from_id:
                        edge = SigiloEdge(
                            from_id=source_id,
                            to_id=edge.to_id,
                            from_column=edge.from_column,
                            to_column=edge.to_column,
                            on_delete=edge.on_delete,
                            edge_type=edge.edge_type,
                        )
                    if edge.from_id in node_ids and edge.to_id in node_ids:
                        edges.append(edge)

        return nodes, edges, schema_names

    def _build_edge(self, foreign_key: ForeignKeyInfo) -> SigiloEdge | None:
        source_id = _make_node_id(foreign_key.source_schema, foreign_key.source_table)
        target_id = _make_node_id(foreign_key.target_schema, foreign_key.target_table)
        if self._schema_filter is not None:
            if foreign_key.source_schema not in self._schema_filter:
                return None
            if foreign_key.target_schema not in self._schema_filter:
                return None
        return SigiloEdge(
            from_id=source_id,
            to_id=target_id,
            from_column=", ".join(foreign_key.source_columns) or None,
            to_column=", ".join(foreign_key.target_columns) or None,
            on_delete=foreign_key.on_delete or None,
            edge_type="inferred" if foreign_key.is_inferred else "declared",
        )

    def _build_config(self, params: StyleParams, schema_names: list[str]) -> SigiloConfig:
        """Build the render config from style params and the included schemas."""

        return SigiloConfig(
            canvas_w=params.canvas_w,
            canvas_h=params.canvas_h,
            style=self._style.value,
            node_r_min=params.node_r_min,
            node_r_max=params.node_r_max,
            node_r_ref_rows=int(params.node_r_scale),
            emit_titles=False,
            emit_macro_rings=params.emit_macro_rings,
            ring_opacity=params.ring_opacity,
            ring_stroke_dash=params.ring_stroke_dash,
            schema_orbit_r=params.schema_orbit_r,
            schema_ring_r_base=params.schema_ring_r_base,
            font_label=params.font_label,
            font_hash=params.font_hash,
            core_label=self._result.database or "database",
            core_subtitle=f"{self._result.engine} @ {self._result.host}",
        )
