"""Integration tests for Phase 10A hybrid QA ranking."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from atlas.ai import AIConfig, AITimeoutError, LocalLLMClient, ModelInfo
from atlas.search import AtlasQA
from tests.integration.phase_10.helpers import build_phase10_result

pytestmark = [pytest.mark.integration, pytest.mark.phase_10a]


class FakeClient(LocalLLMClient):
    """Deterministic local LLM stub for QA tests."""

    def __init__(self, responses: Sequence[object]) -> None:
        super().__init__(AIConfig(provider="ollama", model="llama3"))
        self._responses = list(responses)
        self.prompts: list[str] = []

    def is_available(self) -> bool:
        return True

    def get_model_info(self) -> ModelInfo:
        return ModelInfo("ollama", "llama3", True, "1.0")

    def generate(
        self,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        del max_tokens, temperature
        self.prompts.append(prompt)
        next_value = self._responses.pop(0)
        if isinstance(next_value, Exception):
            raise next_value
        return str(next_value)


def test_qa_ranks_order_headers_first_for_customer_orders_question(phase_tmp_dir: Path) -> None:
    result = build_phase10_result(phase_tmp_dir / "qa_orders.db")
    client = FakeClient(
        [
            """{
                "search_terms": ["customer", "orders"],
                "semantic_terms": ["sales", "transaction", "checkout"],
                "reasoning": "The question targets the transactional order header table.",
                "suggested_query": "SELECT * FROM main.fact_orders LIMIT 20"
            }"""
        ]
    )

    answer = AtlasQA(result, client).ask("where are customer orders stored?")

    assert answer.candidates[0].qualified_name == "main.fact_orders"
    assert answer.candidates[0].semantic_domain == "sales"
    assert answer.suggested_query == "SELECT * FROM main.fact_orders LIMIT 20"
    assert answer.confidence > 0.7


def test_qa_can_rank_semantic_match_even_when_name_tokens_are_distant(phase_tmp_dir: Path) -> None:
    result = build_phase10_result(phase_tmp_dir / "qa_payment.db")
    client = FakeClient(
        [
            """{
                "search_terms": ["billing", "history"],
                "semantic_terms": ["billing", "payment", "gateway", "history"],
                "reasoning": "The question is about payment event history.",
                "suggested_query": null
            }"""
        ]
    )

    answer = AtlasQA(result, client).ask("which table keeps payment history?")

    assert answer.candidates[0].qualified_name == "main.log_payment_history"
    assert answer.candidates[0].semantic_score > 0.7
    assert "payment" in answer.reasoning.casefold()


def test_qa_falls_back_to_structural_tokens_when_llm_times_out(phase_tmp_dir: Path) -> None:
    result = build_phase10_result(phase_tmp_dir / "qa_timeout.db")
    client = FakeClient([AITimeoutError("slow provider")])

    answer = AtlasQA(result, client).ask("find customer accounts")

    assert answer.candidates[0].qualified_name == "main.customer_accounts"
    assert "used only structural tokens" in answer.reasoning
    assert answer.confidence < 0.8


def test_qa_rejects_ungrounded_llm_interpretation_and_falls_back(phase_tmp_dir: Path) -> None:
    result = build_phase10_result(phase_tmp_dir / "qa_ungrounded.db")
    client = FakeClient(
        [
            """{
                "search_terms": ["customer", "orders"],
                "semantic_terms": ["sales", "transaction", "checkout"],
                "reasoning": "The user is asking about where customer orders are stored.",
                "suggested_query": "SELECT * FROM main.fact_orders LIMIT 20"
            }"""
        ]
    )

    answer = AtlasQA(result, client).ask("which table keeps payment history?")

    assert answer.candidates[0].qualified_name == "main.log_payment_history"
    assert "used only structural tokens" in answer.reasoning
    assert answer.suggested_query is None


def test_qa_limits_output_to_top_five_candidates(phase_tmp_dir: Path) -> None:
    result = build_phase10_result(phase_tmp_dir / "qa_top5.db")
    client = FakeClient(
        [
            """{
                "search_terms": ["customer", "order", "payment", "config"],
                "semantic_terms": ["sales", "billing", "crm", "platform"],
                "reasoning": "Broad query touching several functional areas.",
                "suggested_query": null
            }"""
        ]
    )

    answer = AtlasQA(result, client).ask("show customer order payment config objects")

    assert len(answer.candidates) == 5
    assert answer.candidates == sorted(
        answer.candidates,
        key=lambda item: (-item.final_score, -item.semantic_score, -item.structural_score, item.qualified_name),
    )


def test_qa_candidate_reasoning_exposes_component_scores(phase_tmp_dir: Path) -> None:
    result = build_phase10_result(phase_tmp_dir / "qa_reasoning.db")
    client = FakeClient(
        [
            """{
                "search_terms": ["config", "settings"],
                "semantic_terms": ["platform", "configuration"],
                "reasoning": "The user is looking for platform configuration data.",
                "suggested_query": "SELECT * FROM main.config_settings LIMIT 20"
            }"""
        ]
    )

    answer = AtlasQA(result, client).ask("where is the platform configuration?")

    candidate = answer.candidates[0]
    assert candidate.qualified_name == "main.config_settings"
    assert "structural=" in candidate.reasoning
    assert "semantic=" in candidate.reasoning
    assert "heuristic=" in candidate.reasoning


def test_qa_returns_zero_confidence_when_nothing_matches(phase_tmp_dir: Path) -> None:
    result = build_phase10_result(phase_tmp_dir / "qa_none.db")
    client = FakeClient(
        [
            """{
                "search_terms": [],
                "semantic_terms": [],
                "reasoning": "No strong hints were extracted.",
                "suggested_query": null
            }"""
        ]
    )

    answer = AtlasQA(result, client).ask("satellite telemetry archive")

    assert answer.confidence == 0.0
    assert answer.candidates == []
