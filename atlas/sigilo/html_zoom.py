"""Reusable HTML zoom controls for sigilo-based pages."""

from __future__ import annotations

from html import escape

ZOOM_CSS = """
.atlas-zoom-shell{display:grid;grid-template-rows:auto minmax(0,1fr);gap:10px;min-height:0;height:100%;width:100%;min-width:0}
.atlas-zoom-toolbar{display:flex;align-items:center;gap:8px;justify-content:flex-end;flex-wrap:wrap}
.atlas-zoom-toolbar[data-align="left"]{justify-content:flex-start}
.atlas-zoom-label{font-size:.72rem;letter-spacing:.05em;text-transform:uppercase;color:#64748b;font-weight:700;margin-right:auto}
.atlas-zoom-toolbar button{border:1px solid #cbd5e1;background:#fff;color:#0f172a;border-radius:999px;padding:6px 10px;font:inherit;font-size:.82rem;font-weight:700;cursor:pointer}
.atlas-zoom-toolbar button:hover{background:#f8fafc}
.atlas-zoom-toolbar button:active{background:#eef2ff}
.atlas-zoom-readout{font-size:.78rem;color:#475569;min-width:58px;text-align:right}
.atlas-zoom-viewport{overflow:auto;position:relative;min-height:0;min-width:0;height:100%;width:100%;cursor:grab;overscroll-behavior:contain;touch-action:none}
.atlas-zoom-viewport.is-dragging{cursor:grabbing;user-select:none}
.atlas-zoom-sizebox{position:relative;min-width:100%;min-height:100%}
.atlas-zoom-stage{transform-origin:top left;will-change:transform}
.atlas-zoom-stage svg{display:block;width:auto !important;height:auto !important;max-width:none !important;max-height:none !important}
"""


def wrap_zoomable_svg(
    svg_markup: str,
    *,
    label: str = "Sigilo zoom",
    align: str = "right",
    sync_group: str | None = None,
) -> str:
    """Wrap inline SVG markup with a reusable zoom toolbar and viewport."""

    safe_label = escape(label)
    safe_align = "left" if align == "left" else "right"
    sync_attr = f' data-atlas-zoom-sync="{escape(sync_group)}"' if sync_group else ""
    return (
        f'<div class="atlas-zoom-shell" data-atlas-zoom-root{sync_attr}>'
        f'<div class="atlas-zoom-toolbar" data-align="{safe_align}">'
        f'<div class="atlas-zoom-label">{safe_label}</div>'
        '<button type="button" data-atlas-zoom-out aria-label="Zoom out">−</button>'
        '<button type="button" data-atlas-zoom-fit aria-label="Fit sigilo to viewport">Fit</button>'
        '<button type="button" data-atlas-zoom-in aria-label="Zoom in">+</button>'
        '<span class="atlas-zoom-readout" data-atlas-zoom-readout>100%</span>'
        "</div>"
        '<div class="atlas-zoom-viewport" data-atlas-zoom-viewport>'
        '<div class="atlas-zoom-sizebox" data-atlas-zoom-sizebox>'
        f'<div class="atlas-zoom-stage" data-atlas-zoom-stage>{svg_markup}</div>'
        "</div>"
        "</div>"
        "</div>"
    )


def build_zoom_script() -> str:
    """Return client-side zoom logic for every wrapped sigilo viewport."""

    return """
(function() {
  const roots = Array.from(document.querySelectorAll('[data-atlas-zoom-root]'));
  if (!roots.length) return;
  const groupStates = new Map();

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function groupKey(root) {
    return root.getAttribute('data-atlas-zoom-sync') || '';
  }

  function getGroup(root) {
    const key = groupKey(root);
    if (!key) return null;
    if (!groupStates.has(key)) groupStates.set(key, []);
    return groupStates.get(key);
  }

  function scrollRatio(viewport) {
    const maxLeft = Math.max(1, viewport.scrollWidth - viewport.clientWidth);
    const maxTop = Math.max(1, viewport.scrollHeight - viewport.clientHeight);
    return {
      left: viewport.scrollLeft / maxLeft,
      top: viewport.scrollTop / maxTop,
    };
  }

  function applyScrollRatio(viewport, ratio) {
    const maxLeft = Math.max(0, viewport.scrollWidth - viewport.clientWidth);
    const maxTop = Math.max(0, viewport.scrollHeight - viewport.clientHeight);
    viewport.scrollLeft = maxLeft * clamp(ratio.left, 0, 1);
    viewport.scrollTop = maxTop * clamp(ratio.top, 0, 1);
  }

  function initZoom(root) {
    const viewport = root.querySelector('[data-atlas-zoom-viewport]');
    const sizebox = root.querySelector('[data-atlas-zoom-sizebox]');
    const stage = root.querySelector('[data-atlas-zoom-stage]');
    const readout = root.querySelector('[data-atlas-zoom-readout]');
    const zoomIn = root.querySelector('[data-atlas-zoom-in]');
    const zoomOut = root.querySelector('[data-atlas-zoom-out]');
    const zoomFit = root.querySelector('[data-atlas-zoom-fit]');
    const svg = stage ? stage.querySelector('svg') : null;
    if (!viewport || !sizebox || !stage || !readout || !zoomIn || !zoomOut || !zoomFit || !svg) {
      return;
    }

    const viewBox = svg.viewBox && svg.viewBox.baseVal;
    const naturalWidth = Math.max(1, (viewBox && viewBox.width) || svg.getBoundingClientRect().width || 1200);
    const naturalHeight = Math.max(1, (viewBox && viewBox.height) || svg.getBoundingClientRect().height || 1200);
    const minScale = 0.18;
    const maxScale = 4.0;
    let scale = 1.0;
    let dragging = false;
    let dragStartX = 0;
    let dragStartY = 0;
    let dragOriginLeft = 0;
    let dragOriginTop = 0;

    const state = {
      root: root,
      viewport: viewport,
      naturalWidth: naturalWidth,
      naturalHeight: naturalHeight,
      minScale: minScale,
      maxScale: maxScale,
      getScale: function() {
        return scale;
      },
      fitScale: function() {
        const viewportWidth = Math.max(1, viewport.clientWidth - 24);
        const viewportHeight = Math.max(1, viewport.clientHeight - 24);
        return Math.min(maxScale, viewportWidth / naturalWidth, viewportHeight / naturalHeight);
      },
      setScale: function(nextScale) {
        scale = clamp(nextScale, minScale, maxScale);
        root.setAttribute('data-atlas-zoom-scale', scale.toFixed(4));
        sizebox.style.width = (naturalWidth * scale).toFixed(1) + 'px';
        sizebox.style.height = (naturalHeight * scale).toFixed(1) + 'px';
        stage.style.transform = 'scale(' + scale.toFixed(4) + ')';
        readout.textContent = Math.round(scale * 100) + '%';
      },
    };

    const group = getGroup(root);
    if (group) group.push(state);

    function peerStates() {
      return group && group.length ? group : [state];
    }

    function applyScaleToPeers(nextScale, ratio) {
      peerStates().forEach(function(peer) {
        peer.setScale(nextScale);
      });
      if (!ratio) return;
      peerStates().forEach(function(peer) {
        applyScrollRatio(peer.viewport, ratio);
      });
    }

    function syncScale(nextScale, ratio) {
      applyScaleToPeers(nextScale, ratio);
    }

    function fitScaleForPeers() {
      return peerStates().reduce(function(best, peer) {
        return Math.min(best, peer.fitScale());
      }, maxScale);
    }

    function fitPeers() {
      syncScale(fitScaleForPeers(), { left: 0, top: 0 });
    }

    function syncPeerScroll(ratio) {
      peerStates().forEach(function(peer) {
        if (peer.viewport === viewport) return;
        peer.viewport.__atlasZoomSyncing = true;
        applyScrollRatio(peer.viewport, ratio);
        peer.viewport.__atlasZoomSyncing = false;
      });
    }

    function handleDragEnd() {
      dragging = false;
      viewport.classList.remove('is-dragging');
    }

    viewport.addEventListener('mousedown', function(event) {
      if (event.button !== 0) return;
      dragging = true;
      dragStartX = event.clientX;
      dragStartY = event.clientY;
      dragOriginLeft = viewport.scrollLeft;
      dragOriginTop = viewport.scrollTop;
      viewport.classList.add('is-dragging');
      event.preventDefault();
    });

    window.addEventListener('mousemove', function(event) {
      if (!dragging) return;
      viewport.scrollLeft = dragOriginLeft - (event.clientX - dragStartX);
      viewport.scrollTop = dragOriginTop - (event.clientY - dragStartY);
      syncPeerScroll(scrollRatio(viewport));
      event.preventDefault();
    });

    window.addEventListener('mouseup', handleDragEnd);
    viewport.addEventListener('scroll', function() {
      if (viewport.__atlasZoomSyncing) return;
      syncPeerScroll(scrollRatio(viewport));
    }, { passive: true });

    function zoomAroundCenter(multiplier) {
      const ratio = scrollRatio(viewport);
      syncScale(scale * multiplier, ratio);
    }

    zoomIn.addEventListener('click', function() {
      zoomAroundCenter(1.2);
    });
    zoomOut.addEventListener('click', function() {
      zoomAroundCenter(1 / 1.2);
    });
    zoomFit.addEventListener('click', fitPeers);
    window.addEventListener('resize', fitPeers, { passive: true });

    state.setScale(1.0);
    fitPeers();
  }

  roots.forEach(initZoom);
})();
"""


__all__ = ["ZOOM_CSS", "build_zoom_script", "wrap_zoomable_svg"]
