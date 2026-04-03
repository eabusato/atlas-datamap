"""Phase 6A unit tests for heuristic table classification."""

from __future__ import annotations

from atlas.analysis.classifier import PROBABLE_TYPES, TableClassification, TableClassifier
from tests.phase_6_samples import make_column, make_fk, make_result, make_table


def test_probable_types_contract_is_stable() -> None:
    assert PROBABLE_TYPES == (
        "staging",
        "config",
        "pivot",
        "log",
        "fact",
        "domain_main",
        "dimension",
        "unknown",
    )


def test_classify_detects_staging_table() -> None:
    table = make_table(
        "stg_orders_raw",
        row_count=12,
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("batch_id", "text"),
            make_column("load_date", "datetime"),
            make_column("payload_raw", "text"),
        ],
    )

    classification = TableClassifier().classify(table, fk_in_degree=0)

    assert classification.probable_type == "staging"
    assert classification.confidence == 1.0
    assert "staging-like table name" in classification.signals


def test_classify_detects_config_table() -> None:
    table = make_table(
        "app_settings",
        row_count=18,
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("setting_key", "text", nullable=False),
            make_column("setting_value", "text"),
            make_column("description", "text"),
        ],
    )

    classification = TableClassifier().classify(table, fk_in_degree=0)

    assert classification.probable_type == "config"
    assert classification.confidence == 1.0


def test_classify_detects_pivot_table() -> None:
    table = make_table(
        "user_roles_map",
        row_count=300,
        columns=[
            make_column("user_id", "integer", primary_key=True, nullable=False, foreign_key=True),
            make_column("role_id", "integer", primary_key=True, nullable=False, foreign_key=True),
            make_column("created_at", "datetime"),
        ],
        foreign_keys=[
            make_fk("user_roles_map", ["user_id"], "users"),
            make_fk("user_roles_map", ["role_id"], "roles"),
        ],
    )

    classification = TableClassifier().classify(table, fk_in_degree=0)

    assert classification.probable_type == "pivot"
    assert classification.confidence >= 0.8


def test_classify_detects_log_table() -> None:
    table = make_table(
        "audit_log",
        row_count=40_000,
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("customer_id", "integer"),
            make_column("action", "text", nullable=False),
            make_column("event_type", "text", nullable=False),
            make_column("occurred_at", "timestamp", nullable=False),
            make_column("processed_at", "timestamp"),
        ],
    )

    classification = TableClassifier().classify(table, fk_in_degree=0)

    assert classification.probable_type == "log"
    assert classification.confidence == 1.0


def test_classify_detects_fact_table() -> None:
    table = make_table(
        "fact_sales",
        row_count=120_000,
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("customer_id", "integer", nullable=False, foreign_key=True),
            make_column("order_id", "integer", nullable=False, foreign_key=True),
            make_column("product_id", "integer", nullable=False, foreign_key=True),
            make_column("total_amount", "numeric", nullable=False),
            make_column("quantity", "integer", nullable=False),
            make_column("occurred_at", "timestamp", nullable=False),
        ],
        foreign_keys=[
            make_fk("fact_sales", ["customer_id"], "customers"),
            make_fk("fact_sales", ["order_id"], "orders"),
            make_fk("fact_sales", ["product_id"], "products"),
        ],
    )

    classification = TableClassifier().classify(table, fk_in_degree=0)

    assert classification.probable_type == "fact"
    assert classification.confidence == 1.0


def test_classify_detects_fact_table_when_time_grain_is_stored_as_text() -> None:
    table = make_table(
        "fact_orders",
        row_count=8_000,
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("customer_id", "integer", nullable=False, foreign_key=True),
            make_column("total_amount", "numeric", nullable=False),
            make_column("payment_status", "text", nullable=False),
            make_column("created_at", "text", nullable=False),
        ],
        foreign_keys=[
            make_fk("fact_orders", ["customer_id"], "customer_accounts"),
        ],
    )

    classification = TableClassifier().classify(table, fk_in_degree=0)

    assert classification.probable_type == "fact"
    assert classification.confidence >= 0.5


def test_classify_detects_domain_main_table() -> None:
    table = make_table(
        "customers",
        row_count=2_500,
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("name", "text", nullable=False),
            make_column("email", "text", nullable=False),
            make_column("status", "text", nullable=False),
            make_column("created_at", "timestamp", nullable=False),
        ],
    )

    classification = TableClassifier().classify(table, fk_in_degree=4)

    assert classification.probable_type == "domain_main"
    assert classification.confidence == 1.0


def test_classify_detects_dimension_table() -> None:
    table = make_table(
        "dim_products",
        row_count=250,
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("name", "text", nullable=False),
            make_column("description", "text"),
            make_column("category", "text"),
            make_column("sku", "text", nullable=False),
        ],
    )

    classification = TableClassifier().classify(table, fk_in_degree=1)

    assert classification.probable_type == "dimension"
    assert classification.confidence >= 0.8


def test_classify_falls_back_to_unknown_when_confidence_is_low() -> None:
    table = make_table(
        "blob_store",
        row_count=12,
        columns=[
            make_column("payload", "json"),
            make_column("checksum", "binary"),
        ],
    )

    classification = TableClassifier().classify(table, fk_in_degree=0)

    assert classification.probable_type == "unknown"
    assert classification.confidence == 0.0
    assert classification.signals == []


def test_classification_to_dict_rounds_confidence() -> None:
    classification = TableClassification(
        table="orders",
        schema="public",
        probable_type="fact",
        confidence=0.87654,
        signals=["measure-like numeric columns"],
    )

    assert classification.to_dict()["confidence"] == 0.8765


def test_classify_all_mutates_table_metadata() -> None:
    customers = make_table(
        "customers",
        row_count=1_200,
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("name", "text", nullable=False),
            make_column("email", "text", nullable=False),
        ],
    )
    fact_sales = make_table(
        "fact_sales",
        row_count=22_000,
        columns=[
            make_column("id", "integer", primary_key=True, nullable=False),
            make_column("customer_id", "integer", nullable=False, foreign_key=True),
            make_column("product_id", "integer", nullable=False, foreign_key=True),
            make_column("store_id", "integer", nullable=False, foreign_key=True),
            make_column("total_amount", "numeric", nullable=False),
            make_column("occurred_at", "timestamp", nullable=False),
        ],
        foreign_keys=[
            make_fk("fact_sales", ["customer_id"], "customers"),
            make_fk("fact_sales", ["product_id"], "products"),
            make_fk("fact_sales", ["store_id"], "stores"),
        ],
    )
    result = make_result(
        [customers, fact_sales],
        fk_in_degree_map={"public.customers": ["public.orders", "public.fact_sales", "public.audit_log", "public.customer_settings"]},
    )

    classifications = TableClassifier().classify_all(result)

    assert [item.probable_type for item in classifications] == ["domain_main", "fact"]
    assert customers.heuristic_type == "domain_main"
    assert fact_sales.heuristic_type == "fact"
    assert customers.heuristic_confidence > 0.0
    assert fact_sales.heuristic_confidence > 0.0
