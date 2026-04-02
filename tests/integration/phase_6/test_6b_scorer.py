"""Phase 6B integration tests for table relevance scoring."""

from __future__ import annotations

import pytest

from atlas.analysis import TableClassifier, TableScorer
from tests.integration.phase_6.helpers import (
    attach_fill_rate_stats,
    build_analysis_sqlite_fixture,
    introspect_analysis_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_6b]


def test_scorer_ranks_customers_above_staging_table(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)
    attach_fill_rate_stats(result)
    TableClassifier().classify_all(result)

    scores = TableScorer(result).score_all()
    ranked_tables = [item.table for item in scores[:3]]

    assert "customers" in ranked_tables
    assert scores[-1].table == "stg_orders_raw"


def test_scorer_mutates_relevance_score_in_real_result(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)
    TableClassifier().classify_all(result)

    scores = TableScorer(result).score_all()

    customers = result.get_table("main", "customers")
    assert customers is not None
    assert customers.relevance_score == next(item.score for item in scores if item.table == "customers")


def test_scorer_uses_fill_rate_fallback_and_manual_stats(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)
    attach_fill_rate_stats(result)

    scorer = TableScorer(result)
    customers = result.get_table("main", "customers")
    unresolved = result.get_table("main", "unresolved_links")
    assert customers is not None
    assert unresolved is not None

    customer_score = scorer.score_table(customers)
    unresolved_score = scorer.score_table(unresolved)

    assert customer_score.breakdown.fill_rate_score > unresolved_score.breakdown.fill_rate_score


def test_scorer_returns_top_tables_and_clusters(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)
    TableClassifier().classify_all(result)

    scorer = TableScorer(result)
    top_tables = scorer.get_top_tables(2)
    clusters = scorer.get_tables_by_domain_cluster()

    assert len(top_tables) == 2
    assert "domain_main" in clusters
    assert any(item.table == "stg_orders_raw" for item in clusters["staging"])


def test_scorer_respects_schema_filter_on_real_result(phase_tmp_dir) -> None:
    db_path = phase_tmp_dir / "analysis.db"
    build_analysis_sqlite_fixture(db_path)
    result = introspect_analysis_fixture(db_path)

    scores = TableScorer(result).score_all(schema="main")

    assert scores
    assert {item.schema for item in scores} == {"main"}
