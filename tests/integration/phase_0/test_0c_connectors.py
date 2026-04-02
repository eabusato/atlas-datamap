"""Integration coverage for Phase 0C connector and metadata behavior."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors import get_connector
from atlas.connectors.base import PrivacyViolationError
from tests.integration.phase_0.helpers import (
    IntegrationStubConnector,
    build_sqlite_fixture,
    make_config,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_0c]


def test_stub_introspect_schema_populates_metadata() -> None:
    connector = IntegrationStubConnector(make_config())
    with connector.session():
        schema = connector.introspect_schema("public")
    assert schema.table_count == 2
    assert schema.total_size_bytes == 384
    orders = next(table for table in schema.tables if table.name == "orders")
    assert len(orders.columns) == 3


def test_stub_introspect_all_computes_fk_in_degree() -> None:
    connector = IntegrationStubConnector(make_config())
    with connector.session():
        result = connector.introspect_all()
    assert result.fk_in_degree_map["public.customers"] == ["public.orders"]
    customers = result.get_table("public", "customers")
    assert customers is not None
    assert customers.fk_in_degree == 1


def test_stub_masked_sampling_hides_sensitive_columns() -> None:
    connector = IntegrationStubConnector(make_config(privacy_mode=PrivacyMode.masked))
    with connector.session():
        rows = connector.get_sample_rows("public", "customers")
    assert rows[0]["customer_email"] == "***"


def test_stub_no_samples_mode_raises() -> None:
    connector = IntegrationStubConnector(make_config(privacy_mode=PrivacyMode.no_samples))
    with pytest.raises(PrivacyViolationError), connector.session():
        connector.get_sample_rows("public", "customers")


def test_sqlite_connector_end_to_end_introspection(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "integration_phase0.sqlite"
    build_sqlite_fixture(db_path)
    config = AtlasConnectionConfig(
        engine=DatabaseEngine.sqlite,
        host="",
        database=str(db_path),
        privacy_mode=PrivacyMode.masked,
    )
    connector = get_connector(config)
    with connector.session():
        result = connector.introspect_all()
        sample_rows = connector.get_sample_rows("main", "customers")
    assert result.total_tables == 2
    customers = result.get_table("main", "customers")
    assert customers is not None
    assert len(customers.columns) == 2
    assert sample_rows[0]["email"] == "***"
