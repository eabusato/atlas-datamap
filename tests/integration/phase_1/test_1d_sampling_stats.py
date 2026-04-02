"""Integration tests for Phase 1D PostgreSQL sampling and pg_stats usage."""

from __future__ import annotations

import json

import pytest

from atlas.connectors.base import PrivacyViolationError
from atlas.connectors.postgresql import TABLESAMPLE_THRESHOLD, PostgreSQLConnector

pytestmark = pytest.mark.integration


def test_get_sample_rows_returns_real_values_in_normal_mode(
    pg_connector: PostgreSQLConnector,
) -> None:
    rows = pg_connector.get_sample_rows("atlas_test", "customers", limit=2)
    assert rows
    assert any(row["email"] != "***" for row in rows)


def test_get_sample_rows_masks_sensitive_values_in_masked_mode(
    pg_connector_masked: PostgreSQLConnector,
) -> None:
    rows = pg_connector_masked.get_sample_rows("atlas_test", "customers", limit=2)
    assert rows
    assert all(row["email"] == "***" for row in rows)
    assert all(row["cpf"] == "***" if row["cpf"] is not None else True for row in rows)


def test_stats_only_and_no_samples_block_sampling(
    pg_connector_stats_only: PostgreSQLConnector,
    pg_connector_no_samples: PostgreSQLConnector,
) -> None:
    with pytest.raises(PrivacyViolationError):
        pg_connector_stats_only.get_sample_rows("atlas_test", "customers", limit=1)
    with pytest.raises(PrivacyViolationError):
        pg_connector_no_samples.get_sample_rows("atlas_test", "customers", limit=1)


def test_large_table_sampling_path_uses_tablesample(pg_connector: PostgreSQLConnector) -> None:
    assert pg_connector.get_row_count_estimate("atlas_test", "large_events") >= TABLESAMPLE_THRESHOLD
    rows = pg_connector.get_sample_rows("atlas_test", "large_events", limit=50)
    assert len(rows) <= 50
    assert rows


def test_get_column_stats_uses_pg_stats(pg_connector: PostgreSQLConnector) -> None:
    stats = pg_connector.get_column_stats("atlas_test", "customers", "email")
    assert stats.row_count >= 3
    assert stats.distinct_count >= 3
    assert stats.null_count == 0


def test_null_and_distinct_estimates_use_catalog_data(pg_connector: PostgreSQLConnector) -> None:
    assert pg_connector.get_column_null_count("atlas_test", "customers", "cpf") >= 1
    assert pg_connector.get_column_distinct_estimate("atlas_test", "customers", "email") >= 3


def test_introspect_all_returns_json_serializable_result(
    pg_connector: PostgreSQLConnector,
) -> None:
    result = pg_connector.introspect_all()
    payload = json.loads(result.to_json())
    assert payload["engine"] == "postgresql"
    assert any(schema["name"] == "atlas_test" for schema in payload["schemas"])
