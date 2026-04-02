"""Standalone HTML wrapper with an interactive side panel for Atlas SVG output."""

from __future__ import annotations

import json
import re
from html import escape

from atlas.sigilo.html_zoom import ZOOM_CSS, build_zoom_script, wrap_zoomable_svg


class PanelBuilder:
    """Wrap a rendered Atlas SVG in an HTML document with client-side navigation."""

    _CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; overflow: hidden; }
body { display: flex; background: #f7f1e5; color: #0f172a; font-family: "Courier New", monospace; }
#atlas-panel {
  width: 260px;
  min-width: 160px;
  max-width: 400px;
  background: #111827;
  color: #d1d5db;
  display: flex;
  flex-direction: column;
  border-right: 2px solid #374151;
  flex-shrink: 0;
  overflow: hidden;
  resize: horizontal;
}
#atlas-panel-header {
  padding: 10px 12px 8px;
  background: #0f172a;
  border-bottom: 1px solid #374151;
  display: flex;
  align-items: center;
  gap: 8px;
}
#atlas-panel-title {
  font-size: 11px;
  font-weight: bold;
  color: #94a3b8;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
#atlas-search-wrap {
  padding: 8px 10px;
  background: #111827;
  border-bottom: 1px solid #374151;
}
#atlas-search {
  width: 100%;
  background: #1f2937;
  border: 1px solid #374151;
  color: #e5e7eb;
  padding: 5px 8px;
  font-family: inherit;
  font-size: 11px;
  border-radius: 3px;
  outline: none;
}
#atlas-search:focus { border-color: #6b7280; }
#atlas-search::placeholder { color: #4b5563; }
#atlas-stats {
  padding: 4px 12px 6px;
  font-size: 9px;
  color: #6b7280;
  border-bottom: 1px solid #1f2937;
}
#atlas-tree {
  flex: 1;
  overflow: auto;
  padding: 10px 8px 18px;
}
.atlas-schema-group + .atlas-schema-group { margin-top: 10px; }
.atlas-schema-title {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #93c5fd;
  margin-bottom: 4px;
}
.atlas-table-entry {
  width: 100%;
  text-align: left;
  border: 0;
  background: transparent;
  color: #e5e7eb;
  padding: 6px 8px;
  border-radius: 4px;
  cursor: pointer;
  font-family: inherit;
  font-size: 11px;
  display: flex;
  justify-content: space-between;
  gap: 8px;
}
.atlas-table-entry:hover,
.atlas-table-entry.is-active {
  background: #1f2937;
}
#atlas-canvas {
  flex: 1;
  overflow: hidden;
  padding: 12px;
  min-width: 0;
  min-height: 0;
  display: flex;
  flex-direction: column;
}
#atlas-canvas .atlas-zoom-shell { flex: 1; min-height: calc(100vh - 24px); }
#atlas-canvas .atlas-zoom-viewport { height: 100%; min-height: 0; }
#atlas-canvas .system-node-wrap.atlas-selected > circle,
#atlas-canvas .system-node-wrap.atlas-selected > .node-loop-inner {
  stroke: #dc2626 !important;
  stroke-width: 2.6 !important;
  opacity: 1.0 !important;
}
"""

    _HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Atlas — {TITLE}</title>
  <style>{CSS}</style>
</head>
<body>
  <aside id="atlas-panel">
    <header id="atlas-panel-header">
      <div id="atlas-panel-title">{TITLE}</div>
    </header>
    <div id="atlas-search-wrap">
      <input id="atlas-search" type="search" placeholder="Filter tables"/>
    </div>
    <div id="atlas-stats"></div>
    <div id="atlas-tree"></div>
  </aside>
  <main id="atlas-canvas">{SVG_CONTENT}</main>
  <script>{PANEL_SCRIPT}</script>
</body>
</html>
"""

    def __init__(self, svg_bytes: bytes, db_name: str = "") -> None:
        self._svg = self._clean_svg(svg_bytes.decode("utf-8", errors="replace"))
        self._db_name = db_name or "Atlas Datamap"

    def build_html(self) -> str:
        """Return a complete standalone HTML document."""

        return (
            self._HTML_TEMPLATE.replace("{TITLE}", escape(self._db_name))
            .replace("{CSS}", self._CSS + ZOOM_CSS)
            .replace("{SVG_CONTENT}", wrap_zoomable_svg(self._svg, label="Sigilo viewport"))
            .replace("{PANEL_SCRIPT}", self._build_panel_script())
        )

    def _build_panel_script(self) -> str:
        title = json.dumps(self._db_name)
        return f"""
{build_zoom_script()}
(function() {{
  const svg = document.querySelector('#atlas-canvas svg');
  const tree = document.getElementById('atlas-tree');
  const stats = document.getElementById('atlas-stats');
  const search = document.getElementById('atlas-search');
  const dbTitle = {title};
  if (!svg || !tree || !stats || !search) {{
    return;
  }}

  const nodes = Array.from(svg.querySelectorAll('g.system-node-wrap')).map((node, index) => {{
    const schema = node.getAttribute('data-schema') || 'default';
    const table = node.getAttribute('data-table') || ('table_' + index);
    const type = node.getAttribute('data-table-type') || 'table';
    const key = schema + '.' + table;
    node.setAttribute('data-atlas-key', key);
    return {{ key, schema, table, type, element: node }};
  }}).sort((left, right) => left.key.localeCompare(right.key));

  const grouped = new Map();
  nodes.forEach((node) => {{
    if (!grouped.has(node.schema)) {{
      grouped.set(node.schema, []);
    }}
    grouped.get(node.schema).push(node);
  }});

  let activeKey = '';
  let buttons = [];

  function setStats(visible, total) {{
    stats.textContent = dbTitle + '  |  ' + visible + ' / ' + total + ' tables';
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
    if (target) {{
      target.element.scrollIntoView({{ behavior: 'smooth', block: 'center', inline: 'center' }});
    }}
  }}

  function renderTree(filterText) {{
    tree.innerHTML = '';
    buttons = [];
    const query = filterText.trim().toLowerCase();
    let visibleCount = 0;

    grouped.forEach((schemaNodes, schemaName) => {{
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
        button.innerHTML = '<span>' + node.table + '</span><span class="atlas-table-type">' + node.type + '</span>';
        if (node.key === activeKey) {{
          button.classList.add('is-active');
        }}
        button.addEventListener('click', () => focusNode(node.key));
        buttons.push(button);
        section.appendChild(button);
      }});
      tree.appendChild(section);
    }});

    setStats(visibleCount, nodes.length);
  }}

  search.addEventListener('input', () => renderTree(search.value));
  renderTree('');
}})();
"""

    def _clean_svg(self, raw_svg: str) -> str:
        cleaned = re.sub(r"^\s*<\?xml[^>]*>\s*", "", raw_svg, count=1, flags=re.MULTILINE)
        return cleaned.strip()
