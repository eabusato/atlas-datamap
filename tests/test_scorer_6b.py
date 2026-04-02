"""Phase 6B unit tests for heuristic relevance scoring."""

from __future__ import annotations

from atlas.analysis.scorer import (
    ScoreBreakdown,
    TableScorer,
    _score_comment,
    _score_connectivity,
    _score_fill_rate,
    _score_indexes,
    _score_name,
    _score_volume,
)
from atlas.types import ColumnStats
from tests.phase_6_samples import make_column, make_fk, make_index, make_result, make_table


def test_score_breakdown_total_and_dict_rounding() -> None:
    breakdown = ScoreBreakdown(
        volume_score=0.75,
        connectivity_score=0.6,
        fill_rate_score=0.9,
        index_score=1.0,
        name_score=0.8,
        comment_score=1.0,
    )

    assert breakdown.total == 0.77
    assert breakdown.to_dict()["total"] == 0.77


def test_score_volume_uses_defined_buckets() -> None:
    assert _score_volume(0) == 0.0
    assert _score_volume(50) == 0.05
    assert _score_volume(500) == 0.10
    assert _score_volume(5_000) == 0.30
    assert _score_volume(50_000) == 0.55
    assert _score_volume(500_000) == 0.75
    assert _score_volume(2_000_000) == 1.0


def test_connectivity_fill_rate_indexes_name_and_comment_scores() -> None:
    table = make_table(
        "orders",
        columns=[
            make_column(
                "id",
                "integer",
                primary_key=True,
                nullable=False,
                stats=ColumnStats(row_count=100, null_count=0),
            ),
            make_column(
                "notes",
                "text",
                nullable=True,
                stats=ColumnStats(row_count=100, null_count=20),
            ),
            make_column("status", "text", nullable=False),
        ],
        indexes=[
            make_index("orders", ["id"], primary=True),
            make_index("orders", ["status"]),
            make_index("orders", ["notes"], unique=True),
        ],
    )

    assert _score_connectivity(3, 2) == 0.5
    assert _score_fill_rate(table) == 0.9333333333333332
    assert _score_indexes(table) == 1.0
    assert _score_name("orders") == 1.0
    assert _score_name("tmp_orders") == 0.0
    assert _score_name("customer_temp_snapshot") == 0.3
    assert _score_comment("Customer orders") == 1.0
    assert _score_comment("   ") == 0.0


def test_score_table_uses_weighted_breakdown() -> None:
    table = make_table(
        "orders",
        row_count=12_500,
        comment="Customer orders",
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("customer_id", "integer", nullable=False, foreign_key=True),
            make_column("status", "text", nullable=False),
            make_column("notes", "text"),
        ],
        foreign_keys=[make_fk("orders", ["customer_id"], "customers")],
        indexes=[
            make_index("orders", ["id"], primary=True),
            make_index("orders", ["customer_id"]),
        ],
    )
    result = make_result([table], fk_in_degree_map={"public.orders": ["public.order_items"]})

    score = TableScorer(result).score_table(table)

    assert score.qualified_name == "public.orders"
    assert score.score == score.breakdown.total
    assert score.breakdown.volume_score == 0.55
    assert score.breakdown.connectivity_score == 0.2


def test_score_all_ranks_and_mutates_table_relevance() -> None:
    customers = make_table(
        "customers",
        row_count=500_000,
        comment="Customer registry",
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("name", "text", nullable=False),
        ],
        indexes=[make_index("customers", ["id"], primary=True), make_index("customers", ["name"])],
    )
    staging = make_table(
        "stg_customers_tmp",
        row_count=40,
        columns=[make_column("payload", "text"), make_column("load_date", "datetime")],
    )
    result = make_result(
        [customers, staging],
        fk_in_degree_map={"public.customers": ["public.orders", "public.fact_sales"]},
    )

    scores = TableScorer(result).score_all()

    assert [item.table for item in scores] == ["customers", "stg_customers_tmp"]
    assert customers.relevance_score == scores[0].score
    assert staging.relevance_score == scores[1].score
    assert scores[0].rank == 1
    assert scores[1].rank == 2


def test_get_top_tables_and_schema_filter() -> None:
    public_table = make_table(
        "customers",
        row_count=1_000,
        schema="public",
        columns=[make_column("id", "integer", primary_key=True, nullable=False)],
        indexes=[make_index("customers", ["id"], schema="public", primary=True)],
    )
    audit_table = make_table(
        "events",
        row_count=50_000,
        schema="audit",
        columns=[make_column("id", "integer", primary_key=True, nullable=False)],
        indexes=[make_index("events", ["id"], schema="audit", primary=True)],
    )
    result = make_result([public_table], fk_in_degree_map={}, schema="public")
    result.schemas.append(type(result.schemas[0])(name="audit", engine="sqlite", tables=[audit_table]))
    result._compute_summary()
    result._apply_fk_in_degree()

    scorer = TableScorer(result)

    top_public = scorer.get_top_tables(5, schema="public")
    top_all = scorer.get_top_tables(1)

    assert [item.qualified_name for item in top_public] == ["public.customers"]
    assert [item.qualified_name for item in top_all] == ["audit.events"]


def test_get_tables_by_domain_cluster_groups_sorted() -> None:
    customers = make_table(
        "customers",
        row_count=20_000,
        columns=[make_column("id", "integer", primary_key=True, nullable=False)],
        indexes=[make_index("customers", ["id"], primary=True)],
    )
    customers.heuristic_type = "domain_main"
    facts = make_table(
        "fact_sales",
        row_count=200_000,
        columns=[make_column("id", "integer", primary_key=True, nullable=False)],
        indexes=[make_index("fact_sales", ["id"], primary=True)],
    )
    facts.heuristic_type = "fact"
    staging = make_table(
        "tmp_sales",
        row_count=10,
        columns=[make_column("payload", "text")],
    )
    staging.heuristic_type = "staging"
    result = make_result([customers, facts, staging])

    clusters = TableScorer(result).get_tables_by_domain_cluster()

    assert set(clusters) == {"domain_main", "fact", "staging"}
    assert clusters["fact"][0].table == "fact_sales"
