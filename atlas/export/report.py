"""Standalone HTML health report generation for Atlas schema analysis."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from html import escape
from pathlib import Path

import atlas._sigilo as native_sigilo
from atlas.analysis.anomalies import AnomalyDetector, AnomalySeverity, StructuralAnomaly
from atlas.analysis.classifier import TableClassifier
from atlas.analysis.scorer import TableScore, TableScorer
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.sigilo.hover import HoverScriptBuilder
from atlas.sigilo.html_zoom import ZOOM_CSS, build_zoom_script, wrap_zoomable_svg
from atlas.types import IntrospectionResult

_CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;background:#f1f5f9;color:#0f172a;line-height:1.5}
.page{max-width:1280px;margin:0 auto;padding:24px}
.header{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;background:#0f172a;color:#f8fafc;border-radius:14px;padding:24px 28px;margin-bottom:24px}
.header h1{font-size:1.55rem;font-weight:700;margin-bottom:4px}
.header p{font-size:.92rem;color:#cbd5e1}
.meta{font-size:.82rem;color:#94a3b8;text-align:right}
.grid-5{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}
.grid-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}
.card{background:#fff;border-radius:12px;padding:20px;box-shadow:0 1px 2px rgba(15,23,42,.08),0 8px 24px rgba(15,23,42,.06);margin-bottom:18px}
.card h2{font-size:1.05rem;font-weight:700;color:#0f172a;margin-bottom:14px}
table{width:100%;border-collapse:collapse;font-size:.86rem}
th{text-align:left;padding:10px 12px;background:#f8fafc;border-bottom:2px solid #e2e8f0;color:#475569;font-size:.75rem;letter-spacing:.05em;text-transform:uppercase}
td{padding:10px 12px;border-bottom:1px solid #f1f5f9;color:#334155;vertical-align:top}
tr:last-child td{border-bottom:none}
tr:hover td{background:#f8fafc}
.stat{background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:16px;text-align:center}
.stat-value{display:block;font-size:1.85rem;font-weight:700;color:#0f172a}
.stat-label{display:block;margin-top:4px;font-size:.73rem;letter-spacing:.06em;text-transform:uppercase;color:#64748b}
.score-bar{display:inline-block;height:6px;background:#2563eb;border-radius:999px;vertical-align:middle;margin-left:8px}
.type-pill{display:inline-block;padding:4px 8px;border-radius:999px;background:#e2e8f0;color:#334155;font-size:.78rem;font-weight:600;margin:0 6px 6px 0}
.badge{display:inline-block;padding:3px 7px;border-radius:999px;font-size:.72rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase}
.badge-critical{background:#fee2e2;color:#991b1b}
.badge-warning{background:#fef3c7;color:#92400e}
.badge-info{background:#dbeafe;color:#1d4ed8}
.anomaly-summary{font-size:.82rem;color:#64748b;margin-bottom:12px}
.anomaly-row{padding:12px 14px;border-radius:10px;margin-bottom:10px;border-left:4px solid #cbd5e1}
.anomaly-critical{background:#fef2f2;border-left-color:#ef4444}
.anomaly-warning{background:#fffbeb;border-left-color:#f59e0b}
.anomaly-info{background:#eff6ff;border-left-color:#3b82f6}
.anomaly-head{display:flex;gap:10px;align-items:center;justify-content:space-between;margin-bottom:4px}
.anomaly-name{font-weight:700;color:#0f172a}
.anomaly-loc{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.82rem;color:#475569}
.anomaly-desc{font-size:.87rem;color:#1f2937;margin-top:4px}
.anomaly-sugg{font-size:.81rem;color:#64748b;font-style:italic;margin-top:4px}
.svg-container{width:100%;min-height:560px;border:1px solid #e2e8f0;border-radius:12px;background:#fafafa;overflow:hidden;position:relative;padding:12px;display:flex;flex-direction:column}
.svg-container .atlas-zoom-shell{flex:1;min-height:536px}
.svg-container .atlas-zoom-viewport{height:100%;min-height:470px}
.no-sigilo{padding:56px 28px;text-align:center;color:#64748b;font-size:.92rem}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
@media (max-width: 980px){.grid-5,.grid-2{grid-template-columns:1fr}.header{flex-direction:column}.meta{text-align:left}}
"""


def _human_bytes(value: int) -> str:
    if value <= 0:
        return "—"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    return f"{size:.1f} {units[unit_index]}"


def _severity_badge(severity: AnomalySeverity) -> str:
    label = str(severity)
    return f'<span class="badge badge-{label}">{escape(label)}</span>'


def _anomaly_card_class(severity: AnomalySeverity) -> str:
    return f"anomaly-row anomaly-{str(severity)}"


class HTMLReportGenerator:
    """Generate a single-file HTML report for an introspected database."""

    def __init__(self, result: IntrospectionResult) -> None:
        self._result = result

    def generate(
        self,
        output_path: str | Path,
        *,
        include_sigilo: bool = True,
    ) -> None:
        TableClassifier().classify_all(self._result)
        scores = TableScorer(self._result).score_all()
        anomalies = AnomalyDetector().detect(self._result)
        svg_content = self._render_sigilo() if include_sigilo else None

        html = self._render_html(scores=scores, anomalies=anomalies, svg_content=svg_content)
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(html, encoding="utf-8")

    def _render_html(
        self,
        *,
        scores: list[TableScore],
        anomalies: list[StructuralAnomaly],
        svg_content: str | None,
    ) -> str:
        title = escape(self._result.database)
        engine = escape(self._result.engine)
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
        sections = "".join(
            [
                self._section_summary(anomalies),
                self._section_schema_map(),
                self._section_top_tables(scores),
                self._section_type_distribution(),
                self._section_anomalies(anomalies),
                self._section_sigilo(svg_content),
            ]
        )
        hover_js = HoverScriptBuilder()._build_js()
        return (
            "<!DOCTYPE html>\n"
            "<html lang=\"en\">\n"
            "<head>\n"
            "  <meta charset=\"UTF-8\"/>\n"
            "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>\n"
            f"  <title>Atlas Health Report — {title}</title>\n"
            f"  <style>{_CSS}{ZOOM_CSS}</style>\n"
            "</head>\n"
            "<body>\n"
            "  <div class=\"page\">\n"
            "    <div class=\"header\">\n"
            "      <div>\n"
            "        <h1>Atlas Health Report</h1>\n"
            f"        <p>Database: <strong>{title}</strong> ({engine})</p>\n"
            "      </div>\n"
            f"      <div class=\"meta\">Generated at {escape(generated_at)}</div>\n"
            "    </div>\n"
            f"{sections}"
            "  </div>\n"
            f"  <script>{hover_js}\n{build_zoom_script()}</script>\n"
            "</body>\n"
            "</html>\n"
        )

    def _section_summary(self, anomalies: list[StructuralAnomaly]) -> str:
        warning_count = sum(1 for anomaly in anomalies if anomaly.severity >= AnomalySeverity.WARNING)
        return (
            '<div class="card">'
            '<h2>1. Structural Summary</h2>'
            '<div class="grid-5">'
            f'<div class="stat"><span class="stat-value">{len(self._result.schemas)}</span><span class="stat-label">Schemas</span></div>'
            f'<div class="stat"><span class="stat-value">{self._result.total_tables}</span><span class="stat-label">Tables</span></div>'
            f'<div class="stat"><span class="stat-value">{self._result.total_views}</span><span class="stat-label">Views</span></div>'
            f'<div class="stat"><span class="stat-value">{self._result.total_columns}</span><span class="stat-label">Columns</span></div>'
            f'<div class="stat"><span class="stat-value">{warning_count}</span><span class="stat-label">Warnings+</span></div>'
            "</div>"
            f'<p style="margin-top:14px;color:#64748b;font-size:.87rem">Estimated size: {_human_bytes(self._result.total_size_bytes)}</p>'
            "</div>\n"
        )

    def _section_schema_map(self) -> str:
        rows: list[str] = []
        for schema in sorted(self._result.schemas, key=lambda item: item.name):
            tables = sum(1 for table in schema.tables if table.table_type.value == "table")
            views = len(schema.tables) - tables
            columns = sum(len(table.columns) for table in schema.tables)
            row_estimate = sum(table.row_count_estimate for table in schema.tables)
            rows.append(
                "<tr>"
                f"<td><code>{escape(schema.name)}</code></td>"
                f"<td>{tables}</td>"
                f"<td>{views}</td>"
                f"<td>{columns}</td>"
                f"<td>~{row_estimate:,}</td>"
                f"<td>{_human_bytes(schema.total_size_bytes)}</td>"
                "</tr>"
            )
        return (
            '<div class="card">'
            '<h2>2. Schema Map</h2>'
            "<table>"
            "<thead><tr><th>Schema</th><th>Tables</th><th>Views</th><th>Columns</th><th>Est. Rows</th><th>Size</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody>"
            "</table>"
            "</div>\n"
        )

    def _section_top_tables(self, scores: list[TableScore]) -> str:
        top_volume = sorted(scores, key=lambda item: (-item.breakdown.volume_score, item.rank))[:10]
        top_connectivity = sorted(
            scores,
            key=lambda item: (-item.breakdown.connectivity_score, item.rank),
        )[:10]
        return (
            '<div class="grid-2">'
            f'{self._top_table_card("3. Top 10 by Volume", top_volume, "volume")}'
            f'{self._top_table_card("4. Top 10 by Connectivity", top_connectivity, "connectivity")}'
            "</div>\n"
        )

    def _top_table_card(self, title: str, scores: list[TableScore], dimension: str) -> str:
        rows: list[str] = []
        for score in scores:
            table = self._result.get_table(score.schema, score.table)
            assert table is not None
            raw_value = (
                score.breakdown.volume_score
                if dimension == "volume"
                else score.breakdown.connectivity_score
            )
            label = (
                f"~{table.row_count_estimate:,}".replace(",", " ")
                if dimension == "volume"
                else f"{table.fk_in_degree + len(table.foreign_keys)} links"
            )
            rows.append(
                "<tr>"
                f"<td>{score.rank}</td>"
                f"<td><code>{escape(score.qualified_name)}</code></td>"
                f"<td>{escape(label)}</td>"
                f"<td>{raw_value:.2f}<span class=\"score-bar\" style=\"width:{max(8, int(raw_value * 96))}px\"></span></td>"
                "</tr>"
            )
        return (
            '<div class="card">'
            f"<h2>{escape(title)}</h2>"
            "<table>"
            "<thead><tr><th>#</th><th>Table</th><th>Metric</th><th>Score</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody>"
            "</table>"
            "</div>"
        )

    def _section_type_distribution(self) -> str:
        counts = Counter((table.heuristic_type or "unknown") for table in self._result.all_tables())
        pills = "".join(
            f'<span class="type-pill">{escape(name)}: {count}</span>'
            for name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        )
        return (
            '<div class="card">'
            '<h2>5. Heuristic Type Distribution</h2>'
            f"{pills or '<p>No classified tables available.</p>'}"
            "</div>\n"
        )

    def _section_anomalies(self, anomalies: list[StructuralAnomaly]) -> str:
        if not anomalies:
            body = '<p class="anomaly-summary">No structural anomalies were detected.</p>'
        else:
            visible = anomalies[:50]
            counts = Counter(str(anomaly.severity) for anomaly in anomalies)
            summary = ", ".join(
                f"{counts[level]} {level}"
                for level in ("critical", "warning", "info")
                if counts[level]
            )
            hidden = len(anomalies) - len(visible)
            cards = []
            for anomaly in visible:
                cards.append(
                    f'<div class="{_anomaly_card_class(anomaly.severity)}">'
                    '<div class="anomaly-head">'
                    f'<div class="anomaly-name">{escape(anomaly.anomaly_type)}</div>'
                    f"{_severity_badge(anomaly.severity)}"
                    "</div>"
                    f'<div class="anomaly-loc">{escape(anomaly.location)}</div>'
                    f'<div class="anomaly-desc">{escape(anomaly.description)}</div>'
                    f'<div class="anomaly-sugg">{escape(anomaly.suggestion)}</div>'
                    "</div>"
                )
            omission = (
                f'<p class="anomaly-summary">Showing 50 of {len(anomalies)} anomalies. {hidden} omitted.</p>'
                if hidden > 0
                else ""
            )
            body = (
                f'<p class="anomaly-summary">Summary: {escape(summary)}.</p>'
                f"{''.join(cards)}"
                f"{omission}"
            )
        return (
            '<div class="card">'
            f'<h2>6. Structural Anomalies ({len(anomalies)})</h2>'
            f"{body}"
            "</div>\n"
        )

    def _section_sigilo(self, svg_content: str | None) -> str:
        inner = (
            svg_content
            if svg_content
            else (
                '<div class="no-sigilo">'
                "Sigilo unavailable because the native C binding is not loaded or "
                "<code>--no-sigilo</code> was used."
                "</div>"
            )
        )
        return (
            '<div class="card">'
            '<h2>7. Architecture Map (Sigilo)</h2>'
            f'<div id="sigilo-container" class="svg-container">{wrap_zoomable_svg(inner, label="Sigilo viewport", align="left") if svg_content else inner}</div>'
            "</div>\n"
        )

    def _render_sigilo(self) -> str | None:
        if not native_sigilo.available():
            return None
        try:
            return DatamapSigiloBuilder.from_introspection_result(self._result).build().decode("utf-8")
        except Exception:
            return None


__all__ = [
    "HTMLReportGenerator",
    "_anomaly_card_class",
    "_human_bytes",
    "_severity_badge",
]
