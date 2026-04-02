"""Embedded interactive hover tooltip builder for sigilo SVG output."""

from __future__ import annotations

_TOOLTIP_BG = "#f7f1e5"
_TOOLTIP_BORDER = "#1f2937"
_TOOLTIP_TEXT = "#0f172a"
_TOOLTIP_DIM = "#64748b"
_FONT_TITLE = "bold 12.65px monospace"
_FONT_BODY = "11.5px monospace"
_TOOLTIP_PAD_X = 10
_TOOLTIP_PAD_Y = 8
_TOOLTIP_LINE_H = 15
_TOOLTIP_MIN_W = 220
_TOOLTIP_MAX_W = 420


class HoverScriptBuilder:
    """Build the inline SVG script block used for interactive hover tooltips."""

    def build_script(self) -> str:
        js = self._build_js()
        return f"<script type=\"text/javascript\"><![CDATA[\n{js}\n]]></script>\n"

    def _build_js(self) -> str:
        return r"""
(function() {
  'use strict';

  var currentScript = document.currentScript;
  var svg = currentScript && currentScript.closest ? currentScript.closest('svg') : null;
  if (!svg && currentScript && currentScript.ownerSVGElement) {
    svg = currentScript.ownerSVGElement;
  }
  if (!svg) return;

  var vb = svg.viewBox.baseVal;
  var VB_W = vb.width || 1200;
  var VB_H = vb.height || 1200;

  var TIP_PAD_X = """ + str(_TOOLTIP_PAD_X) + r""";
  var TIP_PAD_Y = """ + str(_TOOLTIP_PAD_Y) + r""";
  var TIP_LINE_H = """ + str(_TOOLTIP_LINE_H) + r""";
  var TIP_MIN_W = """ + str(_TOOLTIP_MIN_W) + r""";
  var TIP_MAX_W = """ + str(_TOOLTIP_MAX_W) + r""";
  var TIP_BG = '""" + _TOOLTIP_BG + r"""';
  var TIP_BORDER = '""" + _TOOLTIP_BORDER + r"""';
  var TIP_TEXT = '""" + _TOOLTIP_TEXT + r"""';
  var TIP_DIM = '""" + _TOOLTIP_DIM + r"""';
  var FONT_TITLE = '""" + _FONT_TITLE + r"""';
  var FONT_BODY = '""" + _FONT_BODY + r"""';

  var tip = document.createElementNS('http://www.w3.org/2000/svg', 'g');
  tip.setAttribute('id', 'atlas-tooltip');
  tip.style.pointerEvents = 'none';
  tip.style.display = 'none';
  svg.appendChild(tip);

  var tipInner = document.createElementNS('http://www.w3.org/2000/svg', 'g');
  tip.appendChild(tipInner);

  var tipRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
  tipRect.setAttribute('rx', '3');
  tipRect.setAttribute('fill', TIP_BG);
  tipRect.setAttribute('stroke', TIP_BORDER);
  tipRect.setAttribute('stroke-width', '1');
  tipInner.appendChild(tipRect);

  var tipLines = [];

  function _fmtRows(n) {
    if (n === null || n === undefined || isNaN(n) || n < 0) return '—';
    var s = Math.round(n).toString();
    var out = '';
    for (var i = 0; i < s.length; i++) {
      if (i > 0 && (s.length - i) % 3 === 0) out += '\u202F';
      out += s[i];
    }
    return '~' + out;
  }

  function _fmtBytes(n) {
    if (!n || isNaN(n) || n <= 0) return '—';
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var value = n;
    var unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit += 1;
    }
    return (unit === 0 ? value.toFixed(0) : value.toFixed(1)) + ' ' + units[unit];
  }

  function _fmtType(t) {
    var map = {
      'table': 'table',
      'view': 'view',
      'materialized_view': 'mat. view',
      'foreign_table': 'foreign'
    };
    return map[t] || t || '';
  }

  function _parseColumns(raw) {
    if (!raw) return [];
    return raw.split('||').filter(Boolean).map(function(entry) {
      var parts = entry.split('::');
      return {
        name: parts[0] || '?',
        type: parts[1] || '',
        flags: parts[2] || ''
      };
    });
  }

  function _fmtConfidence(value) {
    if (value === null || value === undefined || value === '') return '';
    var parsed = parseFloat(value);
    if (isNaN(parsed)) return '';
    return Math.round(parsed * 100) + '%';
  }

  function _clearTip() {
    while (tipLines.length) {
      var el = tipLines.pop();
      if (el && el.parentNode) el.parentNode.removeChild(el);
    }
  }

  function _addLine(text, opts) {
    var el = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    el.textContent = text;
    el.setAttribute('fill', opts && opts.dim ? TIP_DIM : TIP_TEXT);
    el.setAttribute('font', opts && opts.title ? FONT_TITLE : FONT_BODY);
    if (opts && opts.title) el.setAttribute('font-weight', 'bold');
    tipInner.appendChild(el);
    tipLines.push(el);
    return el;
  }

  function _measureText(text, opts) {
    var probe = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    probe.textContent = text;
    probe.setAttribute('fill', 'transparent');
    probe.setAttribute('font', opts && opts.title ? FONT_TITLE : FONT_BODY);
    if (opts && opts.title) probe.setAttribute('font-weight', 'bold');
    probe.setAttribute('visibility', 'hidden');
    tipInner.appendChild(probe);
    var width = probe.getComputedTextLength();
    tipInner.removeChild(probe);
    return width;
  }

  function _splitLongToken(token, maxChars, maxWidth, opts) {
    if (!token) return [''];
    var parts = [];
    var index = 0;
    while (index < token.length) {
      var nextIndex = Math.min(token.length, index + maxChars);
      var chunk = token.slice(index, nextIndex);
      while (chunk.length > 1 && _measureText(chunk, opts) > maxWidth) {
        nextIndex -= 1;
        chunk = token.slice(index, nextIndex);
      }
      parts.push(chunk);
      index = nextIndex;
    }
    return parts;
  }

  function _wrapText(text, opts) {
    var content = String(text || '').trim();
    if (!content) {
      _addLine('', opts);
      return;
    }

    var maxChars = opts && opts.title ? 34 : 48;
    var maxWidth = TIP_MAX_W - TIP_PAD_X * 2 - 8;
    var words = content.split(/\s+/);
    var line = '';

    function pushLine(value) {
      _addLine(value, opts);
    }

    for (var i = 0; i < words.length; i++) {
      var word = words[i];
      if (!word) continue;
      if (_measureText(word, opts) > maxWidth) {
        if (line) {
          pushLine(line);
          line = '';
        }
        var chunks = _splitLongToken(word, maxChars, maxWidth, opts);
        for (var j = 0; j < chunks.length; j++) {
          pushLine(chunks[j]);
        }
        continue;
      }
      var candidate = line ? (line + ' ' + word) : word;
      if (
        candidate.length > maxChars
        || _measureText(candidate, opts) > maxWidth
      ) {
        pushLine(line);
        line = word;
      } else {
        line = candidate;
      }
    }

    if (line) pushLine(line);
  }

  function _addSep() {
    var el = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    el.setAttribute('stroke', TIP_BORDER);
    el.setAttribute('stroke-width', '0.5');
    el.setAttribute('opacity', '0.4');
    tipInner.appendChild(el);
    tipLines.push(el);
    return el;
  }

  function _currentZoomScale() {
    var root = svg.closest ? svg.closest('[data-atlas-zoom-root]') : null;
    if (!root) return 1;
    var raw = parseFloat(root.getAttribute('data-atlas-zoom-scale') || '1');
    if (!isFinite(raw) || raw <= 0) return 1;
    return raw;
  }

  function _buildNodeTip(ds) {
    _clearTip();
    _wrapText((ds.table || '?') + '  (' + _fmtType(ds.tableType || ds.table_type) + ')', {title: true});
    _addSep();
    if (ds.schema) _wrapText('schema:   ' + ds.schema);
    _wrapText('rows:     ' + _fmtRows(parseInt(ds.rowEstimate || ds.row_estimate, 10)));
    _wrapText('size:     ' + _fmtBytes(parseInt(ds.sizeBytes || ds.size_bytes, 10)));
    var parts = [];
    if (ds.columnCount || ds.column_count) parts.push('columns: ' + (ds.columnCount || ds.column_count));
    if (ds.fkCount || ds.fk_count) parts.push('FKs: ' + (ds.fkCount || ds.fk_count));
    if (ds.indexCount || ds.index_count) parts.push('indexes: ' + (ds.indexCount || ds.index_count));
    if (parts.length) _wrapText(parts.join(' | '));
    if (ds.comment) _wrapText(ds.comment, {dim: true});
    if (ds.semanticShort || ds.semanticDetailed || ds.semanticRole || ds.semanticDomain) {
      _addSep();
      _wrapText('semantic', {title: true});
      if (ds.semanticShort) _wrapText(ds.semanticShort);
      if (ds.semanticDetailed) _wrapText(ds.semanticDetailed, {dim: true});
      var semanticBits = [];
      if (ds.semanticDomain) semanticBits.push('domain: ' + ds.semanticDomain);
      if (ds.semanticRole) semanticBits.push('role: ' + ds.semanticRole);
      if (semanticBits.length) _wrapText(semanticBits.join(' | '));
      var confidence = _fmtConfidence(ds.semanticConfidence);
      if (confidence) _wrapText('confidence: ' + confidence, {dim: true});
    }
    var columns = _parseColumns(ds.columnsDetail || ds.columns_detail);
    if (columns.length) {
      _addSep();
      _wrapText('columns', {title: true});
      var visible = Math.min(10, columns.length);
      for (var i = 0; i < visible; i++) {
        var suffix = columns[i].flags ? ' [' + columns[i].flags + ']' : '';
        _wrapText('\u2022 ' + columns[i].name + ' : ' + columns[i].type + suffix, {dim: i >= 5});
      }
      if (columns.length > visible) _wrapText('+' + (columns.length - visible) + ' more columns', {dim: true});
    }
  }

  function _buildEdgeTip(ds) {
    _clearTip();
    _wrapText((ds.fkType || ds.fk_type) === 'inferred' ? 'Indirect relationship' : 'Direct relationship', {title: true});
    _addSep();
    var cols = ds.fkColumns || ds.fk_columns || '';
    if (cols) {
      _wrapText(cols.replace(/&gt;/g, '>'));
    } else {
      _wrapText((ds.fkFrom || ds.fk_from || '?') + ' -> ' + (ds.fkTo || ds.fk_to || '?'));
    }
    if (ds.onDelete || ds.on_delete) _wrapText('ON DELETE: ' + (ds.onDelete || ds.on_delete), {dim: true});
  }

  function _buildSchemaTip(ds) {
    _clearTip();
    _wrapText((ds.schema || '?') + '  (schema)', {title: true});
    _addSep();
    if (ds.tableCount) _wrapText('tables:   ' + ds.tableCount);
    if (ds.directFkCount) _wrapText('direct FKs: ' + ds.directFkCount);
    if (ds.totalRows) _wrapText('rows:     ' + _fmtRows(parseInt(ds.totalRows, 10)));
  }

  function _buildColumnTip(ds) {
    _clearTip();
    _wrapText((ds.columnName || '?') + '  (' + (ds.columnRole || ds.semanticRole || 'column') + ')', {title: true});
    _addSep();
    if (ds.schema && ds.table) _wrapText(ds.schema + '.' + ds.table, {dim: true});
    if (ds.columnType) _wrapText('type:     ' + ds.columnType);
    if (ds.columnFlags) _wrapText(ds.columnFlags);
    if (ds.semanticShort || ds.semanticDetailed || ds.semanticRole) {
      _addSep();
      _wrapText('semantic', {title: true});
      if (ds.semanticShort) _wrapText(ds.semanticShort);
      if (ds.semanticDetailed) _wrapText(ds.semanticDetailed, {dim: true});
      if (ds.semanticRole) _wrapText('role:     ' + ds.semanticRole);
      var confidence = _fmtConfidence(ds.semanticConfidence);
      if (confidence) _wrapText('confidence: ' + confidence, {dim: true});
    }
  }

  function _buildColumnLinkTip(ds) {
    _clearTip();
    _wrapText('Internal linkage', {title: true});
    _addSep();
    if (ds.schema && ds.table) _wrapText(ds.schema + '.' + ds.table, {dim: true});
    _wrapText((ds.columnLeft || '?') + ' ↔ ' + (ds.columnRight || '?'));
  }

  function _measureWidth() {
    var width = TIP_MIN_W;
    for (var i = 0; i < tipLines.length; i++) {
      var el = tipLines[i];
      if (el.tagName === 'text') width = Math.max(width, el.getComputedTextLength() + TIP_PAD_X * 2);
    }
    return Math.min(TIP_MAX_W, width);
  }

  function _layoutTip(x, y) {
    var width = _measureWidth();
    var textY = TIP_PAD_Y + 12;
    var sepCount = 0;
    for (var i = 0; i < tipLines.length; i++) {
      var el = tipLines[i];
      if (el.tagName === 'line') {
        el.setAttribute('x1', TIP_PAD_X);
        el.setAttribute('x2', width - TIP_PAD_X);
        el.setAttribute('y1', textY - 8);
        el.setAttribute('y2', textY - 8);
        sepCount += 1;
      } else {
        el.setAttribute('x', TIP_PAD_X);
        el.setAttribute('y', textY);
        textY += TIP_LINE_H;
      }
    }
    var height = Math.max(36, TIP_PAD_Y * 2 + (tipLines.length - sepCount) * TIP_LINE_H + 2);
    tipRect.setAttribute('width', width);
    tipRect.setAttribute('height', height);
    var scale = _currentZoomScale();
    var inverseScale = 1 / scale;
    tipInner.setAttribute('transform', 'scale(' + inverseScale.toFixed(4) + ')');

    var tx = x + 14;
    var ty = y - 8;
    if (tx + width > VB_W) tx = x - width - 14;
    if (tx < 4) tx = 4;
    if (ty + height > VB_H) ty = VB_H - height - 4;
    if (ty < 4) ty = 4;
    tip.setAttribute('transform', 'translate(' + tx + ',' + ty + ')');
  }

  function _show(evt, builder) {
    builder(evt.currentTarget.dataset);
    tip.style.display = 'inline';
    _layoutTip(evt.offsetX || 0, evt.offsetY || 0);
  }

  function _hide() {
    tip.style.display = 'none';
    _clearTip();
  }

  function _move(evt) {
    if (tip.style.display === 'none') return;
    _layoutTip(evt.offsetX || 0, evt.offsetY || 0);
  }

  function _bind(selector, builder) {
    var nodes = svg.querySelectorAll(selector);
    nodes.forEach(function(node) {
      node.addEventListener('mouseenter', function(evt) { _show(evt, builder); });
      node.addEventListener('mouseleave', _hide);
      node.addEventListener('mousemove', _move);
    });
  }

  _bind('.system-node-wrap', _buildNodeTip);
  _bind('.system-edge-wrap', _buildEdgeTip);
  _bind('.system-schema-wrap', _buildSchemaTip);
  _bind('.system-column-wrap', _buildColumnTip);
  _bind('.system-column-link-wrap', _buildColumnLinkTip);
})();
"""
