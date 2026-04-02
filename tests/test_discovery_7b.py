"""Phase 7B unit tests for heuristic discovery."""

from __future__ import annotations

from atlas.search import AtlasDiscovery
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)


def _result() -> IntrospectionResult:
    return IntrospectionResult(
        database="atlas",
        engine="postgresql",
        host="localhost",
        schemas=[
            SchemaInfo(
                name="public",
                engine="postgresql",
                tables=[
                    TableInfo(
                        name="payments",
                        schema="public",
                        table_type=TableType.TABLE,
                        comment="Payment records",
                        heuristic_type="fact",
                        columns=[
                            ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
                            ColumnInfo(name="customer_id", native_type="integer", is_nullable=False),
                        ],
                    ),
                    TableInfo(
                        name="customers",
                        schema="public",
                        table_type=TableType.TABLE,
                        comment="Customer master registry",
                        heuristic_type="dimension",
                        columns=[
                            ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
                        ],
                    ),
                    TableInfo(
                        name="order_payments",
                        schema="public",
                        table_type=TableType.TABLE,
                        comment="Join between orders and payments",
                        heuristic_type="pivot",
                        columns=[
                            ColumnInfo(name="order_id", native_type="integer", is_nullable=False),
                            ColumnInfo(name="payment_id", native_type="integer", is_nullable=False),
                        ],
                        foreign_keys=[
                            ForeignKeyInfo(
                                name="fk_order_payments_payments",
                                source_schema="public",
                                source_table="order_payments",
                                source_columns=["payment_id"],
                                target_schema="public",
                                target_table="payments",
                                target_columns=["id"],
                            )
                        ],
                    ),
                ],
            )
        ],
        fk_in_degree_map={"public.payments": ["public.order_payments"]},
    )


def test_extract_intent_tokens_removes_stop_words() -> None:
    discovery = AtlasDiscovery(_result())

    assert discovery._extract_intent_tokens("onde ficam os pagamentos de clientes") == [
        "pagamentos",
        "clientes",
    ]


def test_expand_tokens_maps_plural_tokens_to_concepts() -> None:
    discovery = AtlasDiscovery(_result())

    expanded = discovery._expand_tokens(["pagamentos", "clientes"])

    assert "pagamento" in expanded
    assert "payment" in expanded["pagamento"]
    assert "cliente" in expanded


def test_search_candidates_accumulates_scores_by_concept() -> None:
    discovery = AtlasDiscovery(_result())

    candidates = discovery._search_candidates(
        {"pagamento": ["payment", "payments"], "cliente": ["customer", "customers"]}
    )

    assert "public.payments" in candidates
    assert candidates["public.payments"].score > 0.0


def test_apply_topology_bonus_marks_fk_hubs() -> None:
    discovery = AtlasDiscovery(_result())
    candidates = discovery._search_candidates({"pagamento": ["payment", "payments", "order"]})
    base_score = candidates["public.payments"].score

    discovery._apply_topology_bonus(candidates)

    assert candidates["public.payments"].topology_bonus is True
    assert candidates["public.payments"].score > base_score


def test_find_likely_location_returns_reasoning_candidates_and_confidence() -> None:
    discovery = AtlasDiscovery(_result())

    result = discovery.find_likely_location("onde ficam os pagamentos de clientes?")

    assert result.candidates
    assert result.candidates[0].qualified_name == "public.payments"
    assert result.confidence > 0.0
    assert "Extracted terms" in result.reasoning


def test_find_likely_location_handles_no_match() -> None:
    discovery = AtlasDiscovery(_result())

    result = discovery.find_likely_location("satellite telemetry orbital drift")

    assert result.candidates == []
    assert result.confidence == 0.0
    assert "No strong candidate tables" in result.reasoning


def test_candidate_ref_to_dict_includes_qualified_name() -> None:
    discovery = AtlasDiscovery(_result())

    result = discovery.find_likely_location("payments")
    payload = result.candidates[0].to_dict()

    assert payload["qualified_name"] == "public.payments"
    assert payload["schema"] == "public"
    assert payload["rank"] == 1
    assert payload["breakdown"] is not None
    assert payload["breakdown"]["volume_score"] >= 0.0


def test_discovery_result_to_dict_serializes_candidates() -> None:
    discovery = AtlasDiscovery(_result())

    result = discovery.find_likely_location("payments")
    payload = result.to_dict()

    assert payload["question"] == "payments"
    assert payload["candidates"]
    assert payload["confidence"] == result.confidence
    assert payload["candidates"][0]["breakdown"] is not None
