"""Helpers for stable SVG baseline comparisons."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

_DB_PATH_RE = re.compile(r"(?:[A-Za-z]:)?/{1,2}[^\"'<>\s]+?\.(?:db|sqlite)")
_TOOLTIP_ID_RE = re.compile(r"\batlas-tooltip-[A-Za-z0-9_-]+\b")
_BUILD_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)


def normalize_svg(svg_text: str) -> str:
    """Normalize non-visual SVG noise while preserving visible structure."""

    cleaned = _BUILD_COMMENT_RE.sub("", svg_text.strip())
    cleaned = _DB_PATH_RE.sub("DATABASE_PATH.db", cleaned)
    cleaned = _TOOLTIP_ID_RE.sub("atlas-tooltip-STABLE", cleaned)
    root = ET.fromstring(cleaned)
    _normalize_element(root)
    return ET.tostring(root, encoding="unicode", method="xml")


def assert_svg_matches_baseline(
    generated_svg: str,
    baseline_path: Path,
    *,
    update: bool,
) -> None:
    """Compare a generated SVG against an approved normalized baseline."""

    normalized = normalize_svg(generated_svg)
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    if update or not baseline_path.exists():
        baseline_path.write_text(normalized, encoding="utf-8")
        return

    expected = baseline_path.read_text(encoding="utf-8")
    if normalized != expected:
        raise AssertionError(
            f"SVG baseline mismatch for {baseline_path}. Re-run with --update-baseline if approved."
        )


def _normalize_element(element: ET.Element) -> None:
    element.attrib = dict(sorted(element.attrib.items()))
    element.text = _normalize_text_node(element.text)
    element.tail = _normalize_text_node(element.tail)
    for child in element:
        _normalize_element(child)


def _normalize_text_node(value: str | None) -> str | None:
    if value is None:
        return None
    if value.strip() == "":
        return None
    return value.strip()
