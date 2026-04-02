"""High-level Python types for sigilo rendering."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class SigiloColumnDesc:
    """Column summary embedded in a rendered sigilo node."""

    name: str
    type_str: str = ""
    is_pk: bool = False
    is_fk: bool = False
    is_nullable: bool = True
    distinct_estimate: int | None = None
    null_rate: float | None = None


@dataclass(slots=True)
class SigiloNode:
    """Graph node representing a table-like object in a sigilo."""

    id: str
    name: str
    schema: str
    node_type: str = "table"
    comment: str | None = None
    row_estimate: int | None = None
    size_bytes: int | None = None
    columns: list[SigiloColumnDesc] = field(default_factory=list)
    fk_count: int = 0
    index_count: int = 0
    r: float = 0.0
    cx: float = 0.0
    cy: float = 0.0


@dataclass(slots=True)
class SigiloEdge:
    """Foreign-key edge between two sigilo nodes."""

    from_id: str
    to_id: str
    from_column: str | None = None
    to_column: str | None = None
    on_delete: str | None = None
    edge_type: str = "declared"


@dataclass(slots=True)
class SigiloConfig:
    """Rendering options shared by the native and Python fallback renderers."""

    canvas_w: float = 1200.0
    canvas_h: float = 1200.0
    canvas_width: float | None = None
    canvas_height: float | None = None
    style: str = "network"
    node_r_min: float = 8.0
    node_r_max: float = 32.0
    node_r_ref_rows: int = 1_000_000
    emit_data_attrs: bool = True
    emit_titles: bool = True
    emit_column_dots: bool = True
    emit_macro_rings: bool = True
    ring_opacity: float = 0.32
    ring_stroke_dash: str = ""
    schema_orbit_r: float = 450.0
    schema_ring_r_base: float = 180.0
    font_label: str = "11px monospace"
    font_hash: str = "9px monospace"
    core_label: str = "database"
    core_subtitle: str = "atlas core"

    def __post_init__(self) -> None:
        if self.canvas_width is not None:
            self.canvas_w = float(self.canvas_width)
        if self.canvas_height is not None:
            self.canvas_h = float(self.canvas_height)
        self.canvas_width = float(self.canvas_w)
        self.canvas_height = float(self.canvas_h)
        self.core_label = str(self.core_label or "database")
        self.core_subtitle = str(self.core_subtitle or "atlas core")
