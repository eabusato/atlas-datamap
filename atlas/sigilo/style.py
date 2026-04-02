"""Visual style presets for Atlas database sigilos."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class SigiloStyle(StrEnum):
    """Visual style variants for datamap sigilos."""

    NETWORK = "network"
    SEAL = "seal"
    COMPACT = "compact"

    @classmethod
    def from_str(cls, value: str) -> SigiloStyle:
        try:
            return cls(value.lower())
        except ValueError as exc:
            valid = ", ".join(style.value for style in cls)
            raise ValueError(f"Invalid sigilo style {value!r}. Expected one of: {valid}.") from exc


@dataclass(frozen=True, slots=True)
class StyleParams:
    """Resolved visual parameters for a sigilo style."""

    canvas_w: float
    canvas_h: float
    ring_style: str
    ring_opacity: float
    ring_stroke_dash: str
    node_r_min: float
    node_r_max: float
    node_r_scale: float
    schema_orbit_r: float
    schema_ring_r_base: float
    font_label: str
    font_hash: str
    emit_macro_rings: bool
    node_r_compact: float


_STYLE_PARAMS: dict[SigiloStyle, StyleParams] = {
    SigiloStyle.NETWORK: StyleParams(
        canvas_w=1200.0,
        canvas_h=1200.0,
        ring_style="macro-ring",
        ring_opacity=0.32,
        ring_stroke_dash="",
        node_r_min=30.0,
        node_r_max=164.0,
        node_r_scale=1_000_000.0,
        schema_orbit_r=450.0,
        schema_ring_r_base=180.0,
        font_label="11px monospace",
        font_hash="9px monospace",
        emit_macro_rings=True,
        node_r_compact=1.0,
    ),
    SigiloStyle.SEAL: StyleParams(
        canvas_w=512.0,
        canvas_h=512.0,
        ring_style="macro-ring",
        ring_opacity=0.20,
        ring_stroke_dash="6 4",
        node_r_min=30.0,
        node_r_max=64.0,
        node_r_scale=500_000.0,
        schema_orbit_r=180.0,
        schema_ring_r_base=140.0,
        font_label="9px monospace",
        font_hash="8px monospace",
        emit_macro_rings=True,
        node_r_compact=1.0,
    ),
    SigiloStyle.COMPACT: StyleParams(
        canvas_w=800.0,
        canvas_h=800.0,
        ring_style="macro-ring",
        ring_opacity=0.0,
        ring_stroke_dash="",
        node_r_min=22.0,
        node_r_max=52.0,
        node_r_scale=1_000_000.0,
        schema_orbit_r=320.0,
        schema_ring_r_base=130.0,
        font_label="8px monospace",
        font_hash="7px monospace",
        emit_macro_rings=False,
        node_r_compact=0.7,
    ),
}


def get_style_params(style: SigiloStyle) -> StyleParams:
    """Return the visual parameters associated with a style enum."""

    return _STYLE_PARAMS[style]
