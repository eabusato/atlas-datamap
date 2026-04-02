"""Executive-style offline HTML reports for Atlas outputs."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from html import escape
from pathlib import Path

from atlas.analysis import AnomalyDetector, TableScorer
from atlas.types import IntrospectionResult

_CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;background:#f4efe4;color:#111827;line-height:1.5}
.page{max-width:1280px;margin:0 auto;padding:24px}
.hero{background:#0f172a;color:#f8fafc;border-radius:18px;padding:24px 28px;margin-bottom:20px}
.hero h1{font-size:1.65rem;margin-bottom:8px}
.hero p{color:#cbd5e1;font-size:.95rem}
.grid-4,.grid-3,.grid-2{display:grid;gap:14px}
.grid-4{grid-template-columns:repeat(4,minmax(0,1fr))}
.grid-3{grid-template-columns:repeat(3,minmax(0,1fr))}
.grid-2{grid-template-columns:repeat(2,minmax(0,1fr))}
.card{background:#fff;border-radius:14px;padding:20px;box-shadow:0 1px 2px rgba(15,23,42,.08),0 10px 30px rgba(15,23,42,.06);margin-bottom:18px}
.card h2{font-size:1.05rem;margin-bottom:12px}
.stat{background:#fff;border-radius:12px;padding:16px;box-shadow:0 1px 2px rgba(15,23,42,.08),0 8px 24px rgba(15,23,42,.06)}
.stat strong{display:block;font-size:1.75rem}
.stat span{font-size:.78rem;text-transform:uppercase;letter-spacing:.06em;color:#64748b}
table{width:100%;border-collapse:collapse;font-size:.88rem}
th,td{text-align:left;padding:10px 12px;border-bottom:1px solid #e5e7eb;vertical-align:top}
th{font-size:.74rem;text-transform:uppercase;letter-spacing:.06em;color:#475569;background:#f8fafc}
tr:last-child td{border-bottom:none}
.pill{display:inline-block;padding:4px 8px;border-radius:999px;font-size:.75rem;font-weight:700}
.pill-critical{background:#fee2e2;color:#991b1b}
.pill-warning{background:#fef3c7;color:#92400e}
.pill-info{background:#dbeafe;color:#1d4ed8}
.muted{color:#6b7280;font-size:.9rem}
.recommendation{padding:12px 14px;border-left:4px solid #1d4ed8;background:#eff6ff;border-radius:10px;margin-bottom:10px}
.recommendation strong{display:block;margin-bottom:4px}
.inventory-item{padding:12px 14px;border:1px solid #e5e7eb;border-radius:10px;margin-bottom:10px}
.inventory-item code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.empty{color:#6b7280;font-size:.9rem}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
@media (max-width: 980px){.grid-4,.grid-3,.grid-2{grid-template-columns:1fr}}
"""


class ExecutiveReportGenerator:
    """Render a concise executive-facing Atlas HTML report."""

    def __init__(
        self,
        result: IntrospectionResult,
        *,
        scores: list[dict[str, object]] | None = None,
        anomalies: list[dict[str, object]] | None = None,
        semantics: dict[str, object] | None = None,
    ) -> None:
        self._result = result
        self._scores = list(scores) if scores is not None else None
        self._anomalies = list(anomalies) if anomalies is not None else None
        self._semantics = semantics or {}

    def build_html(self) -> str:
        """Return the executive report as one standalone HTML document."""

        scores = self._scores or [item.to_dict() for item in TableScorer(self._result).score_all()]
        anomalies = self._anomalies or [item.to_dict() for item in AnomalyDetector().detect(self._result)]
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
        return (
            "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"/>"
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>"
            f"<title>Atlas Executive Report — {escape(self._result.database)}</title>"
            f"<style>{_CSS}</style></head><body><div class=\"page\">"
            f"{self._hero(generated_at)}"
            f"{self._overview_section()}"
            f"{self._schema_section()}"
            f"{self._top_tables_section(scores)}"
            f"{self._anomalies_section(anomalies)}"
            f"{self._recommendations_section(anomalies)}"
            f"{self._semantic_section()}"
            "</div></body></html>"
        )

    def export(self, out_path: str | Path) -> Path:
        """Write the executive report to disk."""

        target = Path(out_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.build_html(), encoding="utf-8")
        return target

    def _hero(self, generated_at: str) -> str:
        return (
            '<section class="hero">'
            "<h1>Atlas Executive Report</h1>"
            f"<p>Database: <strong>{escape(self._result.database)}</strong> ({escape(self._result.engine)})</p>"
            f"<p>Generated at {escape(generated_at)} | Extracted at {escape(self._result.introspected_at or 'unknown')}</p>"
            "</section>"
        )

    def _overview_section(self) -> str:
        semantic_coverage = self._semantic_coverage()
        stats = "".join(
            [
                self._stat("Schemas", len(self._result.schemas)),
                self._stat("Tables", self._result.total_tables),
                self._stat("Columns", self._result.total_columns),
                self._stat("Semantic Coverage", semantic_coverage),
            ]
        )
        return f'<section class="grid-4" style="margin-bottom:18px">{stats}</section>'

    def _schema_section(self) -> str:
        rows = []
        for schema in sorted(self._result.schemas, key=lambda item: item.name):
            schema_rows = sum(table.row_count_estimate for table in schema.tables)
            rows.append(
                "<tr>"
                f"<td><code>{escape(schema.name)}</code></td>"
                f"<td>{len(schema.tables)}</td>"
                f"<td>{schema_rows}</td>"
                f"<td>{schema.total_size_bytes}</td>"
                "</tr>"
            )
        return (
            '<section class="card"><h2>Schemas</h2>'
            "<table><thead><tr><th>Schema</th><th>Tables</th><th>Estimated Rows</th><th>Size Bytes</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></section>"
        )

    def _top_tables_section(self, scores: list[dict[str, object]]) -> str:
        ordered = sorted(
            scores,
            key=lambda item: (
                self._coerce_int(item.get("rank", 999999)),
                -self._coerce_float(item.get("score", 0.0)),
                str(item.get("qualified_name", "")),
            ),
        )[:10]
        rows = []
        for item in ordered:
            rows.append(
                "<tr>"
                f"<td>{escape(str(item.get('rank', '')))}</td>"
                f"<td><code>{escape(str(item.get('qualified_name', '')))}</code></td>"
                f"<td>{escape(str(item.get('score', '')))}</td>"
                f"<td>{escape(str(self._breakdown_value(item, 'volume_score')))}</td>"
                "</tr>"
            )
        return (
            '<section class="card"><h2>Top Tables</h2>'
            "<table><thead><tr><th>Rank</th><th>Table</th><th>Score</th><th>Volume Score</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></section>"
        )

    def _anomalies_section(self, anomalies: list[dict[str, object]]) -> str:
        visible = anomalies[:20]
        if not visible:
            return '<section class="card"><h2>Anomalies</h2><p class="empty">No anomalies detected.</p></section>'
        rows = []
        for item in visible:
            severity = escape(str(item.get("severity", "info")))
            rows.append(
                "<tr>"
                f"<td><span class=\"pill pill-{severity}\">{severity}</span></td>"
                f"<td><code>{escape(str(item.get('location', '')))}</code></td>"
                f"<td>{escape(str(item.get('description', '')))}</td>"
                "</tr>"
            )
        return (
            '<section class="card"><h2>Anomalies</h2>'
            "<table><thead><tr><th>Severity</th><th>Location</th><th>Description</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></section>"
        )

    def _recommendations_section(self, anomalies: list[dict[str, object]]) -> str:
        recommendations = self._recommendations(anomalies)
        if not recommendations:
            return '<section class="card"><h2>Recommendations</h2><p class="empty">No recommendations generated.</p></section>'
        body = "".join(
            (
                '<div class="recommendation">'
                f"<strong>{escape(title)}</strong>"
                f"<div>{escape(description)}</div>"
                "</div>"
            )
            for title, description in recommendations
        )
        return f'<section class="card"><h2>Recommendations</h2>{body}</section>'

    def _semantic_section(self) -> str:
        inventory = self._semantic_inventory()
        if not inventory:
            return ""
        domain_counts = Counter(
            str(item.get("semantic_domain", "unknown"))
            for item in inventory
            if item.get("semantic_domain")
        )
        top_domains = "".join(
            f"<tr><td>{escape(name)}</td><td>{count}</td></tr>"
            for name, count in domain_counts.most_common(8)
        )
        inventory_rows = "".join(
            (
                '<div class="inventory-item">'
                f"<div><code>{escape(str(item['qualified_name']))}</code></div>"
                f"<div>{escape(str(item.get('semantic_short', '')))}</div>"
                f"<div class=\"muted\">Role: {escape(str(item.get('semantic_role', '—')))} | Domain: {escape(str(item.get('semantic_domain', '—')))}</div>"
                "</div>"
            )
            for item in inventory[:12]
        )
        return (
            '<section class="card"><h2>Semantic Coverage</h2>'
            f'<p class="muted" style="margin-bottom:12px">Coverage: {escape(self._semantic_coverage())}</p>'
            '<div class="grid-2">'
            '<div><h3 style="margin-bottom:10px">Top Business Domains</h3>'
            "<table><thead><tr><th>Domain</th><th>Tables</th></tr></thead>"
            f"<tbody>{top_domains}</tbody></table></div>"
            '<div><h3 style="margin-bottom:10px">Executive Table Inventory</h3>'
            f"{inventory_rows}</div>"
            "</div></section>"
        )

    @staticmethod
    def _stat(label: str, value: str | int) -> str:
        return f'<div class="stat"><strong>{escape(str(value))}</strong><span>{escape(label)}</span></div>'

    def _semantic_coverage(self) -> str:
        inventory = self._semantic_inventory()
        if self._result.total_tables <= 0:
            return "0 / 0"
        return f"{len(inventory)} / {self._result.total_tables} tables"

    def _semantic_inventory(self) -> list[dict[str, object]]:
        table_semantics = self._semantics.get("tables", {})
        if isinstance(table_semantics, dict) and table_semantics:
            inventory = []
            for qualified_name, payload in table_semantics.items():
                if not isinstance(payload, dict):
                    continue
                row = {"qualified_name": qualified_name}
                row.update(payload)
                inventory.append(row)
            inventory.sort(key=lambda item: str(item["qualified_name"]))
            return inventory
        inventory = []
        for table in self._result.all_tables():
            if not any(
                [
                    table.semantic_short,
                    table.semantic_detailed,
                    table.semantic_domain,
                    table.semantic_role,
                    table.semantic_confidence > 0.0,
                ]
            ):
                continue
            inventory.append(
                {
                    "qualified_name": table.qualified_name,
                    "semantic_short": table.semantic_short or "",
                    "semantic_domain": table.semantic_domain or "",
                    "semantic_role": table.semantic_role or "",
                    "semantic_confidence": table.semantic_confidence,
                }
            )
        inventory.sort(key=lambda item: str(item["qualified_name"]))
        return inventory

    @staticmethod
    def _recommendations(anomalies: list[dict[str, object]]) -> list[tuple[str, str]]:
        counts = Counter(str(item.get("anomaly_type", "")) for item in anomalies)
        recommendations: list[tuple[str, str]] = []
        if counts.get("no_pk", 0) > 0:
            recommendations.append(
                (
                    "Stabilize table identity",
                    "Prioritize primary keys for tables currently missing stable identifiers.",
                )
            )
        if counts.get("fk_without_index", 0) > 0:
            recommendations.append(
                (
                    "Improve relationship performance",
                    "Add indexes to foreign-key source columns that are currently uncovered.",
                )
            )
        if counts.get("wide_table", 0) > 0:
            recommendations.append(
                (
                    "Review oversized tables",
                    "Split wide tables into clearer bounded responsibilities when practical.",
                )
            )
        if counts.get("ambiguous_column_name", 0) > 0:
            recommendations.append(
                (
                    "Clarify data vocabulary",
                    "Rename generic columns to reflect business meaning and improve discovery quality.",
                )
            )
        if not recommendations and anomalies:
            recommendations.append(
                (
                    "Investigate anomaly clusters",
                    "Review the most frequent anomaly patterns and define schema governance actions.",
                )
            )
        return recommendations[:5]

    @staticmethod
    def _coerce_int(value: object) -> int:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return 0
        return 0

    @staticmethod
    def _coerce_float(value: object) -> float:
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return 0.0
        return 0.0

    @classmethod
    def _breakdown_value(cls, item: dict[str, object], name: str) -> object:
        breakdown = item.get("breakdown", {})
        if not isinstance(breakdown, dict):
            return ""
        return breakdown.get(name, "")
