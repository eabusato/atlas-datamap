"""Standalone offline HTML export for Atlas sigilos."""

from __future__ import annotations

import re
from html import escape
from pathlib import Path

from atlas.sigilo.html_zoom import ZOOM_CSS, build_zoom_script, wrap_zoomable_svg

_SEMANTIC_ATTR_RE = re.compile(r'\sdata-semantic-[a-z0-9\-]+="[^"]*"', re.IGNORECASE)


class StandaloneHTMLBuilder:
    """Wrap an inline Atlas SVG into an offline HTML document."""

    _CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; overflow: hidden; }
body { background: #f7f1e5; color: #0f172a; font-family: "Courier New", monospace; }
.atlas-page { height: 100%; min-height: 100vh; display: grid; grid-template-columns: 360px minmax(0, 1fr); overflow: hidden; }
.atlas-sidebar {
  background: #111827;
  color: #e5e7eb;
  border-right: 2px solid #374151;
  display: grid;
  grid-template-rows: auto auto auto 1fr;
  min-height: 0;
}
.atlas-header { padding: 12px 14px 10px; border-bottom: 1px solid #374151; background: #0f172a; }
.atlas-kicker {
  color: #93c5fd;
  font-size: 10px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin-bottom: 6px;
}
.atlas-title { font-size: 16px; font-weight: bold; color: #f8fafc; }
.atlas-subtitle { margin-top: 4px; color: #94a3b8; font-size: 11px; }
.atlas-search-wrap { padding: 10px 12px; border-bottom: 1px solid #374151; }
.atlas-search {
  width: 100%;
  background: #1f2937;
  border: 1px solid #374151;
  color: #f3f4f6;
  padding: 7px 9px;
  font: inherit;
  font-size: 12px;
  border-radius: 4px;
  outline: none;
}
.atlas-search:focus { border-color: #6b7280; }
.atlas-stats {
  padding: 8px 12px;
  font-size: 10px;
  color: #9ca3af;
  border-bottom: 1px solid #1f2937;
}
.atlas-columns { display: grid; grid-template-columns: minmax(150px, 190px) 1fr; min-height: 0; }
.atlas-tree { overflow: auto; border-right: 1px solid #1f2937; padding: 10px 8px 18px; }
.atlas-details { overflow: auto; padding: 12px; background: #0f172a; }
.atlas-schema-group + .atlas-schema-group { margin-top: 10px; }
.atlas-schema-title {
  color: #93c5fd;
  font-size: 10px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin-bottom: 4px;
}
.atlas-table-entry {
  width: 100%;
  text-align: left;
  border: 0;
  background: transparent;
  color: #f3f4f6;
  padding: 6px 8px;
  border-radius: 4px;
  cursor: pointer;
  font: inherit;
  font-size: 11px;
  display: flex;
  justify-content: space-between;
  gap: 8px;
}
.atlas-table-entry:hover, .atlas-table-entry.is-active { background: #1f2937; }
.atlas-table-type { color: #9ca3af; font-size: 10px; }
.atlas-details-title { color: #f8fafc; font-size: 13px; font-weight: bold; margin-bottom: 8px; }
.atlas-details-empty { color: #6b7280; font-size: 11px; line-height: 1.5; }
.atlas-detail-grid { display: grid; gap: 8px; }
.atlas-detail-row {
  background: rgba(255,255,255,0.03);
  border: 1px solid #1f2937;
  border-radius: 6px;
  padding: 7px 8px;
}
.atlas-detail-label {
  color: #93c5fd;
  font-size: 10px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  margin-bottom: 4px;
}
.atlas-detail-value { color: #e5e7eb; font-size: 12px; line-height: 1.45; word-break: break-word; overflow-wrap:anywhere; white-space:pre-wrap; }
.atlas-canvas {
  overflow: hidden;
  padding: 14px;
  min-height: 0;
  min-width: 0;
  display: flex;
  flex-direction: column;
}
.atlas-canvas .atlas-zoom-shell { flex: 1; min-height: calc(100vh - 28px); }
.atlas-canvas .atlas-zoom-viewport { height: 100%; min-height: 0; }
.atlas-canvas .system-node-wrap.atlas-selected > circle,
.atlas-canvas .system-node-wrap.atlas-selected > .node-loop-inner {
  stroke: #dc2626 !important;
  stroke-width: 2.6 !important;
  opacity: 1.0 !important;
}
@media (max-width: 1080px) {
  .atlas-page { grid-template-columns: 1fr; }
  .atlas-sidebar { min-height: auto; }
  .atlas-columns { grid-template-columns: 1fr; }
  .atlas-tree { max-height: 260px; border-right: 0; border-bottom: 1px solid #1f2937; }
  .atlas-canvas .atlas-zoom-shell { min-height: 70vh; }
}
"""

    _HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>{TITLE}</title>
  <style>{CSS}</style>
</head>
<body>
  <div class="atlas-page">
    <aside class="atlas-sidebar">
      <header class="atlas-header">
        <div class="atlas-kicker">Atlas Standalone Sigilo</div>
        <div class="atlas-title">{TITLE}</div>
        <div class="atlas-subtitle">{SUBTITLE}</div>
      </header>
      <div class="atlas-search-wrap">
        <input id="atlas-search" class="atlas-search" type="search" placeholder="Filter tables"/>
      </div>
      <div id="atlas-stats" class="atlas-stats"></div>
      <div class="atlas-columns">
        <div id="atlas-tree" class="atlas-tree"></div>
        <div class="atlas-details">
          <div class="atlas-details-title">Selection</div>
          <div id="atlas-details-body" class="atlas-details-empty">Select a table from the sigilo or the tree to inspect structural and semantic details.</div>
        </div>
      </div>
    </aside>
    <main id="atlas-canvas" class="atlas-canvas">{SVG}</main>
  </div>
  <script>{SCRIPT}</script>
</body>
</html>
"""

    def __init__(
        self,
        svg_content: str,
        *,
        db_name: str,
        has_semantics: bool = False,
        include_semantics: bool = True,
        title: str | None = None,
    ) -> None:
        if "<svg" not in svg_content:
            raise ValueError("StandaloneHTMLBuilder requires inline SVG content.")
        self._svg_content = svg_content
        self._db_name = db_name
        self._has_semantics = has_semantics
        self._include_semantics = include_semantics
        self._title = title or f"Atlas Export — {db_name}"

    def build_html(self) -> str:
        """Return a complete offline HTML document."""

        effective_has_semantics = self._has_semantics and self._include_semantics
        subtitle = (
            "Semantic metadata detected." if effective_has_semantics else "Structural metadata only."
        )
        svg_content = (
            self._svg_content
            if self._include_semantics
            else self._strip_semantic_metadata(self._svg_content)
        )
        return (
            self._HTML_TEMPLATE.replace("{TITLE}", escape(self._title))
            .replace("{SUBTITLE}", escape(subtitle))
            .replace("{CSS}", self._CSS + ZOOM_CSS)
            .replace("{SVG}", wrap_zoomable_svg(svg_content, label="Sigilo viewport"))
            .replace("{SCRIPT}", self._build_script())
        )

    def export(self, filepath: str | Path) -> Path:
        """Write the standalone document to disk and return the final path."""

        target = Path(filepath)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.build_html(), encoding="utf-8")
        return target

    @staticmethod
    def _strip_semantic_metadata(svg_content: str) -> str:
        return _SEMANTIC_ATTR_RE.sub("", svg_content)

    def _build_script(self) -> str:
        title = self._js_string(self._db_name)
        return f"""
{build_zoom_script()}
(function() {{
  const svg = document.querySelector('#atlas-canvas svg');
  const tree = document.getElementById('atlas-tree');
  const stats = document.getElementById('atlas-stats');
  const detailsBody = document.getElementById('atlas-details-body');
  const search = document.getElementById('atlas-search');
  const dbName = {title};
  if (!svg || !tree || !stats || !detailsBody || !search) {{
    return;
  }}

  const nodes = Array.from(svg.querySelectorAll('.system-node-wrap[data-table]')).map((node, index) => {{
    const schema = node.getAttribute('data-schema') || 'default';
    const table = node.getAttribute('data-table') || ('table_' + index);
    const key = schema + '.' + table;
    node.setAttribute('data-atlas-key', key);
    return {{ key, schema, table, element: node }};
  }}).sort((left, right) => left.key.localeCompare(right.key));

  const schemas = Array.from(svg.querySelectorAll('.system-schema-wrap[data-schema]'));
  const edges = Array.from(svg.querySelectorAll('.system-edge-wrap[data-fk-from]'));
  const schemaGroups = new Map();
  nodes.forEach((node) => {{
    if (!schemaGroups.has(node.schema)) {{
      schemaGroups.set(node.schema, []);
    }}
    schemaGroups.get(node.schema).push(node);
  }});

  let activeKey = '';
  let buttons = [];

  function readValue(element, attrName, fallback) {{
    const value = element.getAttribute(attrName);
    return value && value.trim() ? value : fallback;
  }}

  function metricRows(element) {{
    return [
      ['Qualified Name', readValue(element, 'data-schema', 'default') + '.' + readValue(element, 'data-table', 'unknown')],
      ['Table Type', readValue(element, 'data-table-type', 'table')],
      ['Estimated Rows', readValue(element, 'data-row-estimate', '—')],
      ['Size Bytes', readValue(element, 'data-size-bytes', '—')],
      ['Column Count', readValue(element, 'data-column-count', '0')],
      ['FK Count', readValue(element, 'data-fk-count', '0')],
      ['Index Count', readValue(element, 'data-index-count', '0')],
      ['Comment', readValue(element, 'data-comment', '—')],
      ['Semantic Summary', readValue(element, 'data-semantic-short', '—')],
      ['Semantic Domain', readValue(element, 'data-semantic-domain', '—')],
      ['Semantic Role', readValue(element, 'data-semantic-role', '—')],
    ];
  }}

  function escapeHtml(value) {{
    return value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }}

  function renderDetails(element) {{
    const rows = metricRows(element)
      .map(([label, value]) => (
        '<div class="atlas-detail-row">' +
          '<div class="atlas-detail-label">' + escapeHtml(label) + '</div>' +
          '<div class="atlas-detail-value">' + escapeHtml(String(value)) + '</div>' +
        '</div>'
      ))
      .join('');
    detailsBody.className = 'atlas-detail-grid';
    detailsBody.innerHTML = rows;
  }}

  function focusNode(key) {{
    activeKey = key;
    nodes.forEach((node) => {{
      node.element.classList.toggle('atlas-selected', node.key === key);
    }});
    buttons.forEach((button) => {{
      button.classList.toggle('is-active', button.dataset.atlasKey === key);
    }});
    const target = nodes.find((node) => node.key === key);
    if (!target) {{
      return;
    }}
    renderDetails(target.element);
    target.element.scrollIntoView({{ behavior: 'smooth', block: 'center', inline: 'center' }});
  }}

  function renderTree(filterText) {{
    tree.innerHTML = '';
    buttons = [];
    const query = filterText.trim().toLowerCase();
    let visibleCount = 0;

    schemaGroups.forEach((schemaNodes, schemaName) => {{
      const matches = schemaNodes.filter((node) => {{
        return !query || node.schema.toLowerCase().includes(query) || node.table.toLowerCase().includes(query);
      }});
      if (!matches.length) {{
        return;
      }}
      visibleCount += matches.length;
      const section = document.createElement('section');
      section.className = 'atlas-schema-group';
      const titleNode = document.createElement('div');
      titleNode.className = 'atlas-schema-title';
      titleNode.textContent = schemaName;
      section.appendChild(titleNode);

      matches.forEach((node) => {{
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'atlas-table-entry';
        button.dataset.atlasKey = node.key;
        button.innerHTML = '<span>' + escapeHtml(node.table) + '</span><span class="atlas-table-type">' +
          escapeHtml(readValue(node.element, 'data-table-type', 'table')) + '</span>';
        if (node.key === activeKey) {{
          button.classList.add('is-active');
        }}
        button.addEventListener('click', () => focusNode(node.key));
        buttons.push(button);
        section.appendChild(button);
      }});
      tree.appendChild(section);
    }});

    stats.textContent = dbName + ' | ' + visibleCount + ' / ' + nodes.length + ' tables | ' +
      schemas.length + ' schemas | ' + edges.length + ' relationships';
  }}

  nodes.forEach((node) => {{
    node.element.addEventListener('click', () => focusNode(node.key));
  }});
  search.addEventListener('input', () => renderTree(search.value));
  renderTree('');
  if (nodes.length > 0) {{
    focusNode(nodes[0].key);
  }}
}})();
"""

    @staticmethod
    def _js_string(value: str) -> str:
        escaped = (
            value.replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace('"', '\\"')
        )
        return f'"{escaped}"'
