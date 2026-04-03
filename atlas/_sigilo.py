"""Runtime ABI wrapper around the vendored libatlas_sigilo shared library."""

# Copyright (c) 2026 Erick Andrade Busato
# SPDX-License-Identifier: MIT

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from atlas._sigilo_build import ffi

if TYPE_CHECKING:
    from atlas.sigilo.types import SigiloConfig, SigiloEdge, SigiloNode

_LIB_SUFFIXES = (".dll", ".dylib", ".so")

_lib: Any | None = None
_lib_path: str | None = None
_load_error: str | None = None


def _find_library() -> str | None:
    base = Path(__file__).resolve().parent
    for suffix in _LIB_SUFFIXES:
        candidate = base / f"libatlas_sigilo{suffix}"
        if candidate.exists():
            return str(candidate)
    return None


def _load() -> None:
    global _lib, _lib_path, _load_error
    path = _find_library()
    if path is None:
        _load_error = "libatlas_sigilo not found in package directory"
        return
    try:
        _lib = ffi.dlopen(path)
        _lib_path = path
        _load_error = None
    except Exception as exc:  # pragma: no cover - depends on platform linker failures
        _load_error = str(exc)
        _lib = None
        _lib_path = None


def _require_lib() -> Any:
    if _lib is None:
        raise RuntimeError(f"atlas._sigilo not available: {_load_error}")
    return _lib


def available() -> bool:
    """Return whether the native sigilo shared library is currently loaded."""
    return _lib is not None


def library_path() -> str | None:
    """Return the absolute path to the loaded shared library, when available."""
    return _lib_path


def load_error() -> str | None:
    """Return the most recent library load error, if any."""
    return _load_error


def ping() -> str:
    """Return the ABI version reported by the loaded shared library."""
    lib = _require_lib()
    return cast(bytes, ffi.string(lib.atlas_sigilo_ping())).decode("utf-8")


def render_version() -> str:
    """Return the renderer version exposed by the native shared library."""
    lib = _require_lib()
    return cast(bytes, ffi.string(lib.atlas_render_version())).decode("utf-8")


def _encode_optional(value: str | None) -> bytes:
    return value.encode("utf-8") if value is not None else b""


def _build_c_config(config: SigiloConfig) -> tuple[Any, list[Any]]:
    c_config = ffi.new("atlas_render_config_t *")
    keepalive: list[Any] = []
    style_buf = ffi.new("char[]", config.style.encode("utf-8"))
    ring_dash_buf = ffi.new("char[]", config.ring_stroke_dash.encode("utf-8"))
    font_label_buf = ffi.new("char[]", config.font_label.encode("utf-8"))
    font_hash_buf = ffi.new("char[]", config.font_hash.encode("utf-8"))
    core_label_buf = ffi.new("char[]", config.core_label.encode("utf-8"))
    core_subtitle_buf = ffi.new("char[]", config.core_subtitle.encode("utf-8"))
    keepalive.extend(
        [style_buf, ring_dash_buf, font_label_buf, font_hash_buf, core_label_buf, core_subtitle_buf]
    )
    c_config.canvas_width = int(config.canvas_w)
    c_config.canvas_height = int(config.canvas_h)
    c_config.style = style_buf
    c_config.node_r_min = config.node_r_min
    c_config.node_r_max = config.node_r_max
    c_config.node_r_ref_rows = config.node_r_ref_rows
    c_config.emit_data_attrs = config.emit_data_attrs
    c_config.emit_titles = config.emit_titles
    c_config.emit_column_dots = config.emit_column_dots
    c_config.emit_macro_rings = config.emit_macro_rings
    c_config.ring_opacity = config.ring_opacity
    c_config.ring_stroke_dash = ring_dash_buf
    c_config.schema_orbit_r = config.schema_orbit_r
    c_config.schema_ring_r_base = config.schema_ring_r_base
    c_config.font_label = font_label_buf
    c_config.font_hash = font_hash_buf
    c_config.core_label = core_label_buf
    c_config.core_subtitle = core_subtitle_buf
    return c_config, keepalive


_NODE_TYPE_MAP = {
    "table": 0,
    "view": 1,
    "materialized_view": 2,
    "foreign_table": 3,
}

_EDGE_TYPE_MAP = {
    "declared": 0,
    "inferred": 1,
}


def _build_c_node(node: SigiloNode) -> tuple[Any, list[Any]]:
    c_node = ffi.new("atlas_node_t *")
    keepalive: list[Any] = []
    id_buf = ffi.new("char[]", node.id.encode("utf-8"))
    name_buf = ffi.new("char[]", node.name.encode("utf-8"))
    schema_buf = ffi.new("char[]", node.schema.encode("utf-8"))
    keepalive.extend([id_buf, name_buf, schema_buf])
    c_node.id = id_buf
    c_node.name = name_buf
    c_node.schema = schema_buf
    if node.comment is not None:
        comment_buf = ffi.new("char[]", node.comment.encode("utf-8"))
        keepalive.append(comment_buf)
        c_node.comment = comment_buf
    else:
        c_node.comment = ffi.NULL
    c_node.node_type = _NODE_TYPE_MAP.get(node.node_type, 0)
    c_node.row_estimate = node.row_estimate if node.row_estimate is not None else -1
    c_node.size_bytes = node.size_bytes if node.size_bytes is not None else -1
    c_node.fk_count = node.fk_count
    c_node.index_count = node.index_count
    c_node.schema_group_idx = -1
    c_node.cx = node.cx
    c_node.cy = node.cy
    c_node.r = node.r

    if node.columns:
        columns = ffi.new("atlas_column_desc_t[]", len(node.columns))
        keepalive.append(columns)
        for index, column in enumerate(node.columns):
            name_ptr = ffi.new("char[]", column.name.encode("utf-8"))
            type_ptr = ffi.new("char[]", _encode_optional(column.type_str))
            keepalive.extend([name_ptr, type_ptr])
            columns[index].name = name_ptr
            columns[index].type_str = type_ptr
            columns[index].is_pk = column.is_pk
            columns[index].is_fk = column.is_fk
            columns[index].is_nullable = column.is_nullable
            columns[index].distinct_estimate = (
                column.distinct_estimate if column.distinct_estimate is not None else -1
            )
            columns[index].null_rate = column.null_rate if column.null_rate is not None else -1.0
        c_node.columns = columns
        c_node.column_count = len(node.columns)
    else:
        c_node.columns = ffi.NULL
        c_node.column_count = 0

    return c_node, keepalive


def _build_c_edge(edge: SigiloEdge) -> tuple[Any, list[Any]]:
    c_edge = ffi.new("atlas_edge_t *")
    keepalive: list[Any] = []
    from_buf = ffi.new("char[]", edge.from_id.encode("utf-8"))
    to_buf = ffi.new("char[]", edge.to_id.encode("utf-8"))
    keepalive.extend([from_buf, to_buf])
    c_edge.from_id = from_buf
    c_edge.to_id = to_buf
    if edge.from_column is not None:
        from_column_buf = ffi.new("char[]", edge.from_column.encode("utf-8"))
        keepalive.append(from_column_buf)
        c_edge.from_column = from_column_buf
    else:
        c_edge.from_column = ffi.NULL
    if edge.to_column is not None:
        to_column_buf = ffi.new("char[]", edge.to_column.encode("utf-8"))
        keepalive.append(to_column_buf)
        c_edge.to_column = to_column_buf
    else:
        c_edge.to_column = ffi.NULL
    if edge.on_delete is not None:
        on_delete_buf = ffi.new("char[]", edge.on_delete.encode("utf-8"))
        keepalive.append(on_delete_buf)
        c_edge.on_delete = on_delete_buf
    else:
        c_edge.on_delete = ffi.NULL
    c_edge.edge_type = _EDGE_TYPE_MAP.get(edge.edge_type, 0)
    c_edge.from_idx = -1
    c_edge.to_idx = -1
    return c_edge, keepalive


class RenderContext:
    """Context manager wrapper over the C atlas_render_ctx_t API."""

    def __init__(self, config: SigiloConfig | None = None) -> None:
        self._lib = _require_lib()
        self._ctx = ffi.new("atlas_render_ctx_t *")
        self._disposed = False
        self._keepalive: list[Any] = []
        if config is None:
            self._lib.atlas_render_init(self._ctx, ffi.NULL)
        else:
            c_config, refs = _build_c_config(config)
            self._keepalive.extend([c_config, *refs])
            self._lib.atlas_render_init(self._ctx, c_config)

    def __enter__(self) -> RenderContext:
        return self

    def __exit__(self, *_: object) -> None:
        self.dispose()

    def dispose(self) -> None:
        if not self._disposed:
            self._lib.atlas_render_dispose(self._ctx)
            self._disposed = True
            self._keepalive.clear()

    def add_node(self, node: SigiloNode) -> int:
        c_node, refs = _build_c_node(node)
        idx = self._lib.atlas_render_add_node(self._ctx, c_node)
        refs.clear()
        return -1 if int(idx) == 0xFFFFFFFF else int(idx)

    def add_edge(self, edge: SigiloEdge) -> bool:
        c_edge, refs = _build_c_edge(edge)
        ok = bool(self._lib.atlas_render_add_edge(self._ctx, c_edge))
        refs.clear()
        return ok

    def compute_layout(self) -> None:
        self._lib.atlas_render_compute_layout(self._ctx)

    def compute_layout_force(
        self,
        iterations: int = 300,
        temperature: float = 1.0,
        cooling: float = 0.98,
    ) -> None:
        if iterations < 1:
            raise ValueError("iterations must be >= 1")
        if temperature <= 0.0:
            raise ValueError("temperature must be > 0")
        if not 0.0 < cooling < 1.0:
            raise ValueError("cooling must be in the open interval (0, 1)")
        force_fn = getattr(self._lib, "atlas_render_compute_layout_force", None)
        if force_fn is None:
            self.compute_layout()
            return
        force_fn(self._ctx, iterations, temperature, cooling)
        if self.had_error:
            raise RuntimeError(self.error_message)

    def render(self, *, compute_layout: bool = True) -> bytes:
        if compute_layout:
            self.compute_layout()
        out_len = ffi.new("size_t *")
        buf = self._lib.atlas_render_svg_to_buffer(self._ctx, out_len)
        if buf == ffi.NULL:
            raise RuntimeError(f"atlas_render_svg_to_buffer failed: {self.error_message}")
        try:
            return bytes(ffi.buffer(buf, out_len[0]))
        finally:
            self._lib.free(buf)

    @property
    def had_error(self) -> bool:
        return bool(self._ctx.had_error)

    @property
    def error_message(self) -> str:
        return cast(bytes, ffi.string(self._ctx.error_message)).decode("utf-8")


_load()

__all__ = [
    "RenderContext",
    "available",
    "library_path",
    "load_error",
    "ping",
    "render_version",
]
