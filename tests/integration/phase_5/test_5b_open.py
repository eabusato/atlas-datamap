"""Phase 5B integration tests for the HTML wrapper and local server."""

from __future__ import annotations

import time
from urllib.request import urlopen

import pytest

from atlas.cli.open import AtlasLocalServer
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.sigilo.panel import PanelBuilder
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_5b]


def _result() -> IntrospectionResult:
    customers = TableInfo(
        name="customers",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=3,
        columns=[ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False)],
    )
    orders = TableInfo(
        name="orders",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=5,
        columns=[
            ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="customer_id", native_type="integer", is_nullable=False, is_foreign_key=True),
        ],
        foreign_keys=[
            ForeignKeyInfo(
                name="fk_orders_customer",
                source_schema="public",
                source_table="orders",
                source_columns=["customer_id"],
                target_schema="public",
                target_table="customers",
                target_columns=["id"],
            )
        ],
    )
    return IntrospectionResult(
        database="atlas",
        engine="sqlite",
        host="",
        schemas=[SchemaInfo(name="public", engine="sqlite", tables=[customers, orders])],
    )


def _html() -> str:
    svg = DatamapSigiloBuilder(_result()).build()
    return PanelBuilder(svg, db_name="atlas").build_html()


def _start_server_or_skip(server: AtlasLocalServer):
    try:
        return server.start_in_thread()
    except PermissionError:
        pytest.skip("Local socket binding is not permitted in this environment.")


def test_panel_builder_keeps_svg_data_attributes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    html = _html()

    assert 'class="system-node-wrap"' in html
    assert 'data-schema="public"' in html
    assert 'data-table="orders"' in html


def test_panel_builder_includes_side_panel_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    html = _html()

    assert 'id="atlas-panel"' in html
    assert 'id="atlas-tree"' in html
    assert 'id="atlas-search"' in html


def test_panel_builder_preserves_hover_script(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    html = _html()

    assert "atlas-tooltip" in html
    assert "querySelectorAll('g.system-node-wrap')" in html


def test_local_server_serves_generated_html(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)
    server = AtlasLocalServer(_html(), port=0)
    thread = _start_server_or_skip(server)
    try:
        time.sleep(0.05)
        with urlopen(server.url, timeout=2) as response:
            body = response.read().decode("utf-8")
            assert response.status == 200
            assert "Atlas — atlas" in body
            assert 'id="atlas-panel"' in body
    finally:
        server.stop()
        thread.join(timeout=1)


def test_local_server_favicon_returns_no_content(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)
    server = AtlasLocalServer(_html(), port=0)
    thread = _start_server_or_skip(server)
    try:
        time.sleep(0.05)
        with urlopen(server.url + "favicon.ico", timeout=2) as response:
            assert response.status == 204
            assert response.read() == b""
    finally:
        server.stop()
        thread.join(timeout=1)
