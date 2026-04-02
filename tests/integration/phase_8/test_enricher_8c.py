"""Tests for prompt formatting and semantic enrichment (Phase 8C)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from unittest.mock import patch

import pytest

from atlas.ai import (
    COLUMN_PROMPT_TEMPLATE,
    TABLE_PROMPT_TEMPLATE,
    AIConfig,
    AIConnectionError,
    AIGenerationError,
    AITimeoutError,
    LocalLLMClient,
    ModelInfo,
    ResponseParser,
    SemanticEnricher,
)
from atlas.config import PrivacyMode
from atlas.types import AtlasType, ColumnInfo, ColumnStats, TableInfo, TableType


class FakeClient(LocalLLMClient):
    """Scripted client for deterministic enrichment tests."""

    def __init__(self, responses: Sequence[object]) -> None:
        super().__init__(AIConfig(provider="ollama", model="llama3"))
        self._responses = list(responses)
        self.calls = 0

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
        del prompt, max_tokens, temperature
        self.calls += 1
        next_value = self._responses.pop(0)
        if isinstance(next_value, Exception):
            raise next_value
        return str(next_value)


@pytest.fixture
def table() -> TableInfo:
    return TableInfo(
        name="orders",
        schema="sales",
        table_type=TableType.TABLE,
        row_count_estimate=1234,
        heuristic_type="fact",
        columns=[
            ColumnInfo(
                name="id",
                native_type="integer",
                canonical_type=AtlasType.INTEGER,
                is_primary_key=True,
                is_nullable=False,
            ),
            ColumnInfo(
                name="customer_email",
                native_type="varchar",
                canonical_type=AtlasType.TEXT,
                stats=ColumnStats(row_count=10, distinct_count=10),
            ),
        ],
    )


@pytest.fixture
def column() -> ColumnInfo:
    return ColumnInfo(
        name="customer_email",
        native_type="varchar",
        canonical_type=AtlasType.TEXT,
        stats=ColumnStats(row_count=10, distinct_count=10),
    )


@pytest.fixture
def sample_rows() -> list[dict[str, Any]]:
    return [{"customer_email": "alice@example.com"}]


@pytest.mark.integration
@pytest.mark.phase_8c
class TestResponseParser:
    def test_extracts_plain_json(self) -> None:
        payload = ResponseParser.extract_json('{"short_description": "orders", "confidence": 0.9}')
        assert payload["short_description"] == "orders"

    def test_extracts_markdown_json(self) -> None:
        payload = ResponseParser.extract_json(
            'Here it is:\n```json\n{"probable_role": "dimension"}\n```\nThanks.'
        )
        assert payload["probable_role"] == "dimension"

    def test_extracts_balanced_json_from_wrapped_text(self) -> None:
        payload = ResponseParser.extract_json(
            'Result: {"probable_domain": "finance", "confidence": 1.0} end.'
        )
        assert payload["probable_domain"] == "finance"

    def test_raises_for_missing_json(self) -> None:
        with pytest.raises(AIGenerationError, match="Failed to extract valid JSON"):
            ResponseParser.extract_json("No JSON object here.")


@pytest.mark.integration
@pytest.mark.phase_8c
class TestPromptTemplates:
    def test_table_prompt_formats_with_real_context(self, table: TableInfo) -> None:
        prompt = TABLE_PROMPT_TEMPLATE.format(
            schema=table.schema,
            table_name=table.name,
            table_type=table.table_type.value,
            row_count=str(table.row_count_estimate),
            top_columns_summary="id (integer) [PK, NOT NULL]",
            fk_summary="none declared",
            heuristic_classification="fact",
        )
        assert "Table: sales.orders" in prompt
        assert '"probable_domain": "Primary business domain"' in prompt

    def test_column_prompt_formats_with_real_context(self) -> None:
        prompt = COLUMN_PROMPT_TEMPLATE.format(
            schema="sales",
            table_name="orders",
            column_name="customer_email",
            native_type="varchar",
            nullable="True",
            distinct="10",
            null_rate="0.0%",
            pattern="EMAIL",
            samples="['[PATTERN: EMAIL]']",
        )
        assert "Parent Table: sales.orders" in prompt
        assert '"probable_role": "Functional role of the column"' in prompt


@pytest.mark.integration
@pytest.mark.phase_8c
class TestSemanticEnricher:
    def test_enrich_table_updates_semantic_fields(
        self,
        table: TableInfo,
        sample_rows: list[dict[str, Any]],
    ) -> None:
        client = FakeClient(
            [
                '{"short_description": "Orders", "detailed_description": "Order facts", '
                '"probable_domain": "Sales", "probable_role": "Fact table", "confidence": 0.91}'
            ]
        )
        payload = SemanticEnricher(client).enrich_table(table, sample_rows, PrivacyMode.normal)
        assert payload["probable_domain"] == "Sales"
        assert table.semantic_short == "Orders"
        assert table.semantic_domain == "Sales"
        assert table.semantic_role == "Fact table"
        assert table.semantic_confidence == pytest.approx(0.91)

    def test_enrich_column_updates_semantic_fields(
        self,
        table: TableInfo,
        column: ColumnInfo,
        sample_rows: list[dict[str, Any]],
    ) -> None:
        client = FakeClient(
            [
                '{"short_description": "Customer email", "detailed_description": "Primary email", '
                '"probable_role": "customer identifier", "confidence": 0.88}'
            ]
        )
        payload = SemanticEnricher(client).enrich_column(
            table, column, sample_rows, PrivacyMode.normal
        )
        assert payload["probable_role"] == "customer identifier"
        assert column.semantic_short == "Customer email"
        assert column.semantic_role == "customer identifier"
        assert column.semantic_confidence == pytest.approx(0.88)

    def test_timeout_retries_with_exponential_backoff(
        self,
        table: TableInfo,
        sample_rows: list[dict[str, Any]],
    ) -> None:
        client = FakeClient(
            [
                AITimeoutError("first timeout"),
                AITimeoutError("second timeout"),
                '{"short_description": "Orders", "detailed_description": "Recovered", '
                '"probable_domain": "Sales", "probable_role": "Fact", "confidence": 0.7}',
            ]
        )
        with patch("atlas.ai.enricher.time.sleep") as sleep_mock:
            SemanticEnricher(client).enrich_table(table, sample_rows, PrivacyMode.normal)
        assert client.calls == 3
        assert sleep_mock.call_args_list[0].args == (1.0,)
        assert sleep_mock.call_args_list[1].args == (2.0,)

    def test_non_timeout_connection_error_is_not_retried(
        self,
        table: TableInfo,
        sample_rows: list[dict[str, Any]],
    ) -> None:
        client = FakeClient([AIConnectionError("offline")])
        with (
            patch("atlas.ai.enricher.time.sleep") as sleep_mock,
            pytest.raises(AIConnectionError, match="offline"),
        ):
            SemanticEnricher(client).enrich_table(table, sample_rows, PrivacyMode.normal)
        assert client.calls == 1
        sleep_mock.assert_not_called()

    def test_invalid_confidence_falls_back_to_zero(
        self,
        table: TableInfo,
        sample_rows: list[dict[str, Any]],
    ) -> None:
        client = FakeClient(
            [
                '{"short_description": "Orders", "detailed_description": "desc", '
                '"probable_domain": "Sales", "probable_role": "Fact", "confidence": "bad"}'
            ]
        )
        SemanticEnricher(client).enrich_table(table, sample_rows, PrivacyMode.normal)
        assert table.semantic_confidence == 0.0
