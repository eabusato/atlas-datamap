"""HTML report generator for Atlas snapshot diffs."""

from __future__ import annotations

from collections.abc import Sequence
from html import escape
from pathlib import Path

from atlas.export.diff import SchemaDiff
from atlas.export.snapshot import AtlasSnapshot
from atlas.sigilo.html_zoom import ZOOM_CSS, build_zoom_script, wrap_zoomable_svg

_CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;background:#f8fafc;color:#0f172a;line-height:1.45}
.page{max-width:1800px;margin:0 auto;padding:24px}
.hero{background:#0f172a;color:#f8fafc;border-radius:16px;padding:24px 28px;margin-bottom:20px}
.hero h1{font-size:1.5rem;margin-bottom:8px}
.hero p{color:#cbd5e1;font-size:.92rem}
.grid{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:12px;margin-bottom:20px}
.card{background:#fff;border-radius:12px;padding:20px;box-shadow:0 1px 2px rgba(15,23,42,.08),0 8px 24px rgba(15,23,42,.06);margin-bottom:18px}
.stat{background:#fff;border-radius:12px;padding:16px;box-shadow:0 1px 2px rgba(15,23,42,.08),0 8px 24px rgba(15,23,42,.06)}
.stat strong{display:block;font-size:1.8rem;color:#0f172a}
.stat span{font-size:.78rem;text-transform:uppercase;letter-spacing:.06em;color:#64748b}
h2{font-size:1.02rem;margin-bottom:12px}
table{width:100%;border-collapse:collapse;font-size:.88rem}
th,td{text-align:left;padding:10px 12px;border-bottom:1px solid #e2e8f0;vertical-align:top}
th{font-size:.74rem;text-transform:uppercase;letter-spacing:.06em;color:#475569;background:#f8fafc}
tr:last-child td{border-bottom:none}
.empty{color:#64748b;font-size:.9rem}
.pill{display:inline-block;padding:3px 8px;border-radius:999px;font-size:.75rem;font-weight:700}
.plus{background:#dcfce7;color:#166534}
.minus{background:#fee2e2;color:#991b1b}
.warn{background:#fef3c7;color:#92400e}
.grid-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}
.grid-2>div{min-width:0}
.sigilo-wrap{border:1px solid #e2e8f0;border-radius:12px;background:#fafafa;overflow:hidden;min-height:560px;padding:12px}
.sigilo-wrap .atlas-zoom-shell{height:536px}
.sigilo-wrap .atlas-zoom-viewport{height:100%;min-height:470px}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
@media (max-width: 980px){.grid,.grid-2{grid-template-columns:1fr}}
"""


def _summary_row(label: str, value: int) -> str:
    return f'<div class="stat"><strong>{value}</strong><span>{escape(label)}</span></div>'


def _summary_text_row(label: str, value: str) -> str:
    return f'<div class="stat"><strong>{escape(value)}</strong><span>{escape(label)}</span></div>'


def _estimated_total_rows(snapshot: AtlasSnapshot) -> int:
    return sum(table.row_count_estimate for table in snapshot.result.all_tables())


def _format_rows(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def _table_block(headers: tuple[str, ...], rows: Sequence[tuple[str, ...]]) -> str:
    if not rows:
        return '<p class="empty">No changes detected in this section.</p>'
    header_html = "".join(f"<th>{escape(header)}</th>" for header in headers)
    row_html = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{header_html}</tr></thead><tbody>{row_html}</tbody></table>"


class SnapshotDiffReport:
    """Render a standalone HTML diff report from two Atlas snapshots."""

    def render(
        self,
        before: AtlasSnapshot,
        after: AtlasSnapshot,
        diff: SchemaDiff,
    ) -> str:
        before_rows = _estimated_total_rows(before)
        after_rows = _estimated_total_rows(after)
        net_row_delta = after_rows - before_rows
        summary = "".join(
            [
                _summary_row("Added Tables", len(diff.added_tables)),
                _summary_row("Removed Tables", len(diff.removed_tables)),
                _summary_row("Type Changes", len(diff.type_changes)),
                _summary_row("Relation Changes", len(diff.new_relations) + len(diff.removed_relations)),
                _summary_text_row("Rows Before", _format_rows(before_rows)),
                _summary_text_row("Rows After", _format_rows(after_rows)),
                _summary_text_row("Net Row Delta", f"{net_row_delta:+,}".replace(",", " ")),
            ]
        )
        return (
            "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"/>"
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>"
            "<title>Atlas Snapshot Diff</title>"
            f"<style>{_CSS}{ZOOM_CSS}</style></head><body><div class=\"page\">"
            f"{self._hero(before, after)}"
            f"<div class=\"grid\">{summary}</div>"
            f"{self._tables_section(diff)}"
            f"{self._columns_section(diff)}"
            f"{self._types_section(diff)}"
            f"{self._volume_section(diff)}"
            f"{self._relations_section(diff)}"
            f"{self._sigilo_section(before, after)}"
            f"</div><script>{build_zoom_script()}</script></body></html>"
        )

    def write(
        self,
        before: AtlasSnapshot,
        after: AtlasSnapshot,
        diff: SchemaDiff,
        output_path: str | Path,
    ) -> Path:
        target = Path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.render(before, after, diff), encoding="utf-8")
        return target

    @staticmethod
    def _hero(before: AtlasSnapshot, after: AtlasSnapshot) -> str:
        return (
            '<section class="hero">'
            "<h1>Atlas Snapshot Diff</h1>"
            f"<p>Before: <code>{escape(before.manifest.database)}</code> @ {escape(before.manifest.created_at)}</p>"
            f"<p>After: <code>{escape(after.manifest.database)}</code> @ {escape(after.manifest.created_at)}</p>"
            "<p>This report compares two offline .atlas snapshots. In daily use, "
            "the normal flow is: scan or onboard today, save a new snapshot, and compare it "
            "against the latest local snapshot from a previous run.</p>"
            "</section>"
        )

    def _tables_section(self, diff: SchemaDiff) -> str:
        rows = [
            ('<span class="pill plus">added</span>', f"<code>{escape(name)}</code>")
            for name in diff.added_tables
        ] + [
            ('<span class="pill minus">removed</span>', f"<code>{escape(name)}</code>")
            for name in diff.removed_tables
        ]
        return f'<section class="card"><h2>Tables</h2>{_table_block(("Change", "Table"), rows)}</section>'

    def _columns_section(self, diff: SchemaDiff) -> str:
        rows: list[tuple[str, ...]] = []
        for table, columns in sorted(diff.added_columns.items()):
            for column in columns:
                rows.append(
                    (
                        '<span class="pill plus">added</span>',
                        f"<code>{escape(table)}</code>",
                        f"<code>{escape(column)}</code>",
                    )
                )
        for table, columns in sorted(diff.removed_columns.items()):
            for column in columns:
                rows.append(
                    (
                        '<span class="pill minus">removed</span>',
                        f"<code>{escape(table)}</code>",
                        f"<code>{escape(column)}</code>",
                    )
                )
        return (
            f'<section class="card"><h2>Columns</h2>'
            f'{_table_block(("Change", "Table", "Column"), rows)}</section>'
        )

    def _types_section(self, diff: SchemaDiff) -> str:
        rows = [
            (
                f"<code>{escape(change.table)}</code>",
                f"<code>{escape(change.column)}</code>",
                f"<code>{escape(change.old_type)}</code>",
                f"<code>{escape(change.new_type)}</code>",
            )
            for change in diff.type_changes
        ]
        return (
            f'<section class="card"><h2>Physical Type Changes</h2>'
            f'{_table_block(("Table", "Column", "Old", "New"), rows)}</section>'
        )

    def _volume_section(self, diff: SchemaDiff) -> str:
        rows = [
            (
                f"<code>{escape(change.table)}</code>",
                str(change.old_rows),
                str(change.new_rows),
                f'<span class="pill warn">{change.percent_change:+.2f}%</span>',
            )
            for change in diff.volume_changes
        ]
        return (
            f'<section class="card"><h2>Volume Changes</h2>'
            f'{_table_block(("Table", "Before", "After", "Delta"), rows)}</section>'
        )

    def _relations_section(self, diff: SchemaDiff) -> str:
        rows = [
            (
                '<span class="pill plus">added</span>',
                f"<code>{escape(relation.source_ref)}</code>",
                f"<code>{escape(relation.target_ref)}</code>",
            )
            for relation in diff.new_relations
        ] + [
            (
                '<span class="pill minus">removed</span>',
                f"<code>{escape(relation.source_ref)}</code>",
                f"<code>{escape(relation.target_ref)}</code>",
            )
            for relation in diff.removed_relations
        ]
        return (
            f'<section class="card"><h2>Relations</h2>'
            f'{_table_block(("Change", "From", "To"), rows)}</section>'
        )

    def _sigilo_section(self, before: AtlasSnapshot, after: AtlasSnapshot) -> str:
        return (
            '<section class="card"><h2>Comparative Sigilo</h2><div class="grid-2">'
            f'<div><h3 style="margin-bottom:10px">Before</h3><div class="sigilo-wrap">{wrap_zoomable_svg(before.sigil_svg, label="Before sigilo", align="left", sync_group="snapshot-diff")}</div></div>'
            f'<div><h3 style="margin-bottom:10px">After</h3><div class="sigilo-wrap">{wrap_zoomable_svg(after.sigil_svg, label="After sigilo", align="left", sync_group="snapshot-diff")}</div></div>'
            "</div></section>"
        )
