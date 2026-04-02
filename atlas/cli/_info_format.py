"""Formatters for ``atlas info`` output."""

from __future__ import annotations

import json
from typing import Any

from atlas.types import TableInfo

_SIZE_UNITS = [("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)]
_YAML_SPECIAL = frozenset(":{}[]|>&*!,%@`#'\"")


def _fmt_bytes(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "?"
    for unit, threshold in _SIZE_UNITS:
        if size_bytes >= threshold:
            return f"{size_bytes / threshold:.1f} {unit}"
    return f"{size_bytes} B"


def _fmt_rows(row_count: int) -> str:
    if row_count <= 0:
        return "?"
    digits = str(row_count)
    groups: list[str] = []
    while len(digits) > 3:
        groups.append(digits[-3:])
        digits = digits[:-3]
    groups.append(digits)
    return "\u202f".join(reversed(groups))


def _bool_mark(value: bool) -> str:
    return "✓" if value else ""


def _trunc(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"


def render_text(
    info: TableInfo,
    *,
    include_columns: bool = True,
    include_fks: bool = True,
    include_indexes: bool = True,
) -> str:
    """Render a human-friendly text summary for a single table."""

    lines: list[str] = []
    qualified = f"{info.schema}.{info.name}"
    row_str = f"~{_fmt_rows(info.row_count_estimate)}"
    size_str = _fmt_bytes(info.size_bytes)
    col_count = len(info.columns) if info.columns else info.column_count
    type_str = info.table_type.value.replace("_", " ")
    summary = f"type: {type_str}  │  rows: {row_str}  │  size: {size_str}  │  cols: {col_count}"
    width = max(len(qualified), len(summary)) + 4
    border = "─" * width

    lines.append(f"┌{border}┐")
    lines.append(f"│ {qualified:<{width - 2}} │")
    lines.append(f"│ {summary:<{width - 2}} │")
    lines.append(f"└{border}┘")

    if info.comment:
        lines.append(f"Comment: {info.comment}")

    if include_columns and info.columns:
        lines.append("")
        lines.append(f"COLUMNS ({len(info.columns)})")
        name_width = min(max(len("Name"), max(len(column.name) for column in info.columns)), 24)
        type_width = min(max(len("Type"), max(len(column.native_type) for column in info.columns)), 24)
        header = (
            f"  {'#':>3}  "
            f"{'Name':<{name_width}}  "
            f"{'Type':<{type_width}}  "
            f"{'Null':<5}  "
            f"{'PK':<3}  "
            f"{'FK':<3}  "
            f"Default"
        )
        lines.append(header)
        lines.append("  " + "─" * (len(header) - 2))
        for index, column in enumerate(info.columns, start=1):
            lines.append(
                "  "
                f"{index:>3}  "
                f"{_trunc(column.name, name_width):<{name_width}}  "
                f"{_trunc(column.native_type, type_width):<{type_width}}  "
                f"{'YES' if column.is_nullable else 'NO ':<5}  "
                f"{_bool_mark(column.is_primary_key):<3}  "
                f"{_bool_mark(column.is_foreign_key):<3}  "
                f"{column.default_value or ''}"
            )

    if include_fks and info.foreign_keys:
        lines.append("")
        lines.append(f"FOREIGN KEYS ({len(info.foreign_keys)})")
        for foreign_key in info.foreign_keys:
            src_cols = ", ".join(foreign_key.source_columns)
            tgt_cols = ", ".join(foreign_key.target_columns)
            on_delete = (
                f"  ON DELETE {foreign_key.on_delete}" if foreign_key.on_delete != "NO ACTION" else ""
            )
            kind = "[inferred]" if foreign_key.is_inferred else "[declared]"
            lines.append(
                "  "
                f"{foreign_key.source_schema}.{foreign_key.source_table}({src_cols}) → "
                f"{foreign_key.target_schema}.{foreign_key.target_table}({tgt_cols})"
                f"{on_delete}  {kind}"
            )

    if include_indexes and info.indexes:
        lines.append("")
        lines.append(f"INDEXES ({len(info.indexes)})")
        name_width = min(max(len("Name"), max(len(index.name) for index in info.indexes)), 40)
        for item in info.indexes:
            flags: list[str] = []
            if item.is_primary:
                flags.append("PRIMARY")
            if item.is_unique:
                flags.append("UNIQUE")
            if item.is_partial:
                flags.append("PARTIAL")
            flags.append(item.index_type)
            lines.append(
                "  "
                f"{_trunc(item.name, name_width):<{name_width}}  "
                f"({', '.join(item.columns)})  "
                f"{'  '.join(flags)}"
            )

    return "\n".join(lines)


def render_json(
    info: TableInfo,
    *,
    include_columns: bool = True,
    include_fks: bool = True,
    include_indexes: bool = True,
    indent: int = 2,
) -> str:
    """Render a ``TableInfo`` payload as pretty JSON."""

    payload = info.to_dict()
    if not include_columns:
        payload.pop("columns", None)
    if not include_fks:
        payload.pop("foreign_keys", None)
    if not include_indexes:
        payload.pop("indexes", None)
    return json.dumps(payload, ensure_ascii=False, indent=indent)


def render_yaml(
    info: TableInfo,
    *,
    include_columns: bool = True,
    include_fks: bool = True,
    include_indexes: bool = True,
) -> str:
    """Render a ``TableInfo`` payload as YAML, with an internal fallback serializer."""

    payload = info.to_dict()
    if not include_columns:
        payload.pop("columns", None)
    if not include_fks:
        payload.pop("foreign_keys", None)
    if not include_indexes:
        payload.pop("indexes", None)
    try:
        import yaml  # type: ignore[import-untyped]

        return str(
            yaml.dump(
                payload,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            )
        )
    except ImportError:
        return _minimal_yaml(payload)


def _minimal_yaml(obj: Any, indent: int = 0) -> str:
    pad = "  " * indent
    if isinstance(obj, dict):
        if not obj:
            return "{}\n"
        parts: list[str] = []
        for key, value in obj.items():
            if isinstance(value, (dict, list)):
                if isinstance(value, list) and not value:
                    parts.append(f"{pad}{key}: []\n")
                elif isinstance(value, dict) and not value:
                    parts.append(f"{pad}{key}: {{}}\n")
                else:
                    parts.append(f"{pad}{key}:\n{_minimal_yaml(value, indent + 1)}")
            else:
                parts.append(f"{pad}{key}: {_yaml_scalar(value)}\n")
        return "".join(parts)
    if isinstance(obj, list):
        if not obj:
            return ""
        list_parts: list[str] = []
        for item in obj:
            if isinstance(item, dict):
                rendered_lines = _minimal_yaml(item, indent + 1).splitlines()
                if not rendered_lines:
                    list_parts.append(f"{pad}- {{}}\n")
                    continue
                list_parts.append(f"{pad}- {rendered_lines[0].lstrip()}\n")
                for line in rendered_lines[1:]:
                    list_parts.append(f"{line}\n")
            elif isinstance(item, list):
                rendered_block = _minimal_yaml(item, indent + 1)
                list_parts.append(f"{pad}-\n{rendered_block}")
            else:
                list_parts.append(f"{pad}- {_yaml_scalar(item)}\n")
        return "".join(list_parts)
    return f"{pad}{_yaml_scalar(obj)}\n"


def _yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    needs_quote = (
        not text
        or text.lower() in {"true", "false", "null", "yes", "no", "on", "off", "~"}
        or text[0].isdigit()
        or any(char in text for char in _YAML_SPECIAL)
        or text.startswith("-")
        or "\n" in text
    )
    if needs_quote:
        escaped = text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'
    return text
