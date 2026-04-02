"""Command implementation for ``atlas open``."""

from __future__ import annotations

import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import click

from atlas.cli._common import require_existing_path
from atlas.sigilo.panel import PanelBuilder


class AtlasLocalServer:
    """Serve an in-memory Atlas HTML document over stdlib HTTP."""

    def __init__(self, html_content: str, port: int = 8421) -> None:
        self._html_bytes = html_content.encode("utf-8")
        self._requested_port = port
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        if self._server is None:
            return self._requested_port
        return int(self._server.server_port)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/"

    def start(self) -> None:
        """Open the browser and block until interrupted."""

        self._ensure_server()
        server = self._server
        assert server is not None
        webbrowser.open(self.url)
        click.echo(f"[atlas open] Serving {self.url}", err=True)
        try:
            server.serve_forever(poll_interval=0.2)
        except KeyboardInterrupt:
            click.echo("[atlas open] Stopping local server.", err=True)
        finally:
            self.stop()

    def start_in_thread(self) -> threading.Thread:
        """Start the server on a background thread for tests."""

        if self._thread is not None and self._thread.is_alive():
            return self._thread
        self._ensure_server()
        server = self._server
        assert server is not None
        self._thread = threading.Thread(target=server.serve_forever, daemon=True)
        self._thread.start()
        return self._thread

    def stop(self) -> None:
        """Shutdown the in-memory local server."""

        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._server = None

    def _ensure_server(self) -> None:
        if self._server is None:
            self._server = HTTPServer(("127.0.0.1", self._requested_port), self._build_handler())

    def _build_handler(self) -> type[BaseHTTPRequestHandler]:
        html_bytes = self._html_bytes

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/favicon.ico":
                    self.send_response(204)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html_bytes)))
                self.end_headers()
                self.wfile.write(html_bytes)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

        return _Handler


def _read_svg(path: Path) -> bytes:
    payload = path.read_bytes()
    if b"<svg" not in payload:
        raise click.UsageError(f"{path} does not contain inline SVG content.")
    return payload


@click.command("open")
@click.argument("sigil_path", type=click.Path(path_type=Path))
@click.option("--port", default=8421, show_default=True, type=int, help="Local HTTP server port.")
def open_cmd(sigil_path: Path, port: int) -> None:
    """Open a rendered data map in a local browser session."""

    svg_path = require_existing_path(sigil_path)
    svg_bytes = _read_svg(svg_path)
    html = PanelBuilder(svg_bytes, db_name=svg_path.stem).build_html()
    server = AtlasLocalServer(html, port=port)
    server.start()
