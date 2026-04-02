"""Phase 7B integration tests for heuristic discovery over a real SQLite fixture."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas.search import AtlasDiscovery
from tests.integration.phase_7.helpers import (
    build_phase7_sqlite_fixture,
    introspect_phase7_sqlite,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_7b]


def _discovery_from_fixture(db_path: Path) -> AtlasDiscovery:
    build_phase7_sqlite_fixture(db_path)
    return AtlasDiscovery(introspect_phase7_sqlite(db_path))


def test_discovery_finds_payment_history_table_in_real_fixture(phase_tmp_dir: Path) -> None:
    discovery = _discovery_from_fixture(phase_tmp_dir / "payment_history.db")

    result = discovery.find_likely_location("where is the payment history stored?")

    assert result.candidates
    assert result.candidates[0].qualified_name == "main.log_payment_history"
    assert result.confidence > 0.0


def test_discovery_finds_customer_accounts_in_real_fixture(phase_tmp_dir: Path) -> None:
    discovery = _discovery_from_fixture(phase_tmp_dir / "customers.db")

    result = discovery.find_likely_location("where are the customer accounts?")

    assert any(candidate.qualified_name == "main.customer_accounts" for candidate in result.candidates)
    assert "customer" in result.reasoning.lower()


def test_discovery_finds_order_items_in_real_fixture(phase_tmp_dir: Path) -> None:
    discovery = _discovery_from_fixture(phase_tmp_dir / "order_items.db")

    result = discovery.find_likely_location("show me the order item details")

    assert result.candidates
    assert result.candidates[0].qualified_name == "main.order_items"
    assert "item" in result.candidates[0].justification


def test_discovery_finds_config_settings_in_real_fixture(phase_tmp_dir: Path) -> None:
    discovery = _discovery_from_fixture(phase_tmp_dir / "config.db")

    result = discovery.find_likely_location("where are the billing settings and configuration?")

    assert result.candidates
    assert result.candidates[0].qualified_name == "main.config_settings"
    assert result.confidence > 0.0


def test_discovery_orders_hub_tables_before_isolated_matches(phase_tmp_dir: Path) -> None:
    discovery = _discovery_from_fixture(phase_tmp_dir / "hub.db")

    result = discovery.find_likely_location("customer order flow")
    by_name = {candidate.qualified_name: candidate for candidate in result.candidates}

    assert "main.fact_orders" in by_name
    assert "main.customer_accounts" in by_name
    assert "hub FK bonus" in by_name["main.fact_orders"].justification


def test_discovery_returns_zero_confidence_for_irrelevant_query(phase_tmp_dir: Path) -> None:
    discovery = _discovery_from_fixture(phase_tmp_dir / "none.db")

    result = discovery.find_likely_location("orbital telemetry plasma chamber")

    assert result.candidates == []
    assert result.confidence == 0.0
    assert "No strong candidate tables" in result.reasoning
