"""Phase 5B unit tests for the local HTML viewer."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from atlas.cli.open import AtlasLocalServer, open_cmd
from atlas.sigilo.panel import PanelBuilder

_SVG = b"""
<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg">
  <g class="system-node-wrap" data-schema="public" data-table="orders" data-table-type="table">
    <circle cx="10" cy="10" r="5"/>
  </g>
  <g class="system-node-wrap" data-schema="public" data-table="customers" data-table-type="table">
    <circle cx="20" cy="20" r="5"/>
  </g>
</svg>
"""


def test_panel_builder_wraps_svg_in_html() -> None:
    html = PanelBuilder(_SVG, db_name="atlas").build_html()

    assert "<!DOCTYPE html>" in html
    assert '<aside id="atlas-panel">' in html
    assert '<main id="atlas-canvas">' in html
    assert '<svg xmlns="http://www.w3.org/2000/svg">' in html


def test_panel_builder_strips_xml_declaration() -> None:
    html = PanelBuilder(_SVG, db_name="atlas").build_html()

    assert "<?xml" not in html


def test_panel_builder_embeds_search_and_panel_script() -> None:
    html = PanelBuilder(_SVG, db_name="atlas").build_html()

    assert 'id="atlas-search"' in html
    assert "querySelectorAll('g.system-node-wrap')" in html
    assert "renderTree('')" in html
    assert "data-atlas-zoom-in" in html
    assert "data-atlas-zoom-fit" in html
    assert "#atlas-canvas .atlas-zoom-shell { flex: 1; min-height: calc(100vh - 24px); }" in html
    assert "#atlas-canvas .atlas-zoom-viewport { height: 100%; min-height: 0; }" in html


def test_local_server_starts_background_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    class _FakeHTTPServer:
        def __init__(self, address: tuple[str, int], handler: object) -> None:
            self.server_port = 9911
            self.address = address
            self.handler = handler

        def serve_forever(self, poll_interval: float = 0.2) -> None:
            events.append(f"serve:{poll_interval}")

        def shutdown(self) -> None:
            events.append("shutdown")

        def server_close(self) -> None:
            events.append("close")

    monkeypatch.setattr("atlas.cli.open.HTTPServer", _FakeHTTPServer)

    server = AtlasLocalServer("<html><body>ok</body></html>", port=0)
    thread = server.start_in_thread()
    thread.join(timeout=1)

    assert server.url == "http://127.0.0.1:9911/"
    assert events == ["serve:0.2"]


def test_local_server_stop_shuts_down_bound_server(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    class _FakeHTTPServer:
        def __init__(self, address: tuple[str, int], handler: object) -> None:
            self.server_port = 9922

        def serve_forever(self, poll_interval: float = 0.2) -> None:
            return None

        def shutdown(self) -> None:
            events.append("shutdown")

        def server_close(self) -> None:
            events.append("close")

    monkeypatch.setattr("atlas.cli.open.HTTPServer", _FakeHTTPServer)

    server = AtlasLocalServer("<html><body>ok</body></html>", port=0)
    server.start_in_thread().join(timeout=1)
    server.stop()

    assert events == ["shutdown", "close"]


def test_open_cmd_reads_svg_and_starts_server(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    svg_path = tmp_path / "atlas.svg"
    svg_path.write_bytes(_SVG)
    captured: dict[str, object] = {}

    class _FakeServer:
        def __init__(self, html_content: str, port: int = 8421) -> None:
            captured["html"] = html_content
            captured["port"] = port

        def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr("atlas.cli.open.AtlasLocalServer", _FakeServer)

    result = CliRunner().invoke(open_cmd, [str(svg_path), "--port", "9999"])

    assert result.exit_code == 0, result.output
    assert captured["started"] is True
    assert captured["port"] == 9999
    assert "Atlas — atlas" in str(captured["html"])


def test_open_cmd_rejects_non_svg_file(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.txt"
    payload_path.write_text("not svg", encoding="utf-8")

    result = CliRunner().invoke(open_cmd, [str(payload_path)])

    assert result.exit_code != 0
    assert "does not contain inline SVG content" in result.output
