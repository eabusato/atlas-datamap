"""Integration tests for Phase 10B vector search."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from atlas.ai import AIConfig, AIGenerationError, EmbeddingGenerator, LocalLLMClient, ModelInfo
from atlas.search.vector import VectorSearch
from tests.integration.phase_10.helpers import build_phase10_result

pytestmark = [pytest.mark.integration, pytest.mark.phase_10b]


def _make_mock_response(body: dict[str, Any] | bytes) -> MagicMock:
    response = MagicMock()
    response.read.return_value = (
        json.dumps(body).encode("utf-8") if isinstance(body, dict) else body
    )
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    return response


class FakeClient(LocalLLMClient):
    """Fake embedding-capable client for deterministic vector tests."""

    def __init__(self, provider: str = "ollama") -> None:
        super().__init__(AIConfig(provider=provider, model="mini-embed"))

    def is_available(self) -> bool:
        return True

    def get_model_info(self) -> ModelInfo:
        return ModelInfo(self.config.provider, self.config.model, True, "1.0")

    def generate(
        self,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        del prompt, max_tokens, temperature
        raise AssertionError("Text generation is not expected in vector tests.")


class FakeEmbeddingGenerator(EmbeddingGenerator):
    """Generator that returns precomputed vectors without network access."""

    def __init__(self, vectors: dict[str, Sequence[float]], provider: str = "ollama") -> None:
        super().__init__(FakeClient(provider=provider))
        self._vectors = {key: [float(value) for value in values] for key, values in vectors.items()}

    def generate_embedding(self, text: str) -> list[float]:
        for marker, vector in self._vectors.items():
            if marker in text:
                return list(vector)
        raise AssertionError(f"Unexpected embedding request: {text}")


def test_cosine_similarity_returns_one_for_identical_vectors() -> None:
    similarity = VectorSearch.cosine_similarity([1.0, 2.0], [1.0, 2.0])
    assert similarity == pytest.approx(1.0)


def test_cosine_similarity_returns_zero_for_zero_magnitude_vector() -> None:
    similarity = VectorSearch.cosine_similarity([0.0, 0.0], [1.0, 2.0])
    assert similarity == 0.0


def test_cosine_similarity_returns_zero_for_different_vector_sizes() -> None:
    similarity = VectorSearch.cosine_similarity([1.0, 2.0], [1.0])
    assert similarity == 0.0


def test_vector_search_returns_nearest_semantic_neighbor(phase_tmp_dir: Path) -> None:
    result = build_phase10_result(phase_tmp_dir / "vector_rank.db")
    generator = FakeEmbeddingGenerator(
        {
            "fact_orders": [1.0, 0.0, 0.0],
            "log_payment_history": [0.0, 1.0, 0.0],
            "customer_accounts": [0.0, 0.0, 1.0],
            "order_items": [0.8, 0.1, 0.0],
            "config_settings": [0.0, 0.2, 0.8],
            "payment history": [0.0, 1.0, 0.0],
        }
    )
    index = VectorSearch(generator)
    index.build_from_result(result)

    matches = index.search("payment history", top_k=2)

    assert matches[0].qualified_name == "main.log_payment_history"
    assert matches[0].similarity == pytest.approx(1.0)
    assert len(matches) == 2


def test_vector_search_persists_and_reloads_entries(phase_tmp_dir: Path) -> None:
    result = build_phase10_result(phase_tmp_dir / "vector_save.db")
    generator = FakeEmbeddingGenerator(
        {
            "fact_orders": [1.0, 0.0],
            "log_payment_history": [0.0, 1.0],
            "customer_accounts": [0.6, 0.4],
            "order_items": [0.9, 0.1],
            "config_settings": [0.2, 0.8],
        }
    )
    path = phase_tmp_dir / "atlas.embeddings"
    index = VectorSearch(generator)
    index.build_from_result(result)

    index.save(path)
    restored = VectorSearch.load(path, generator)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["version"] == 1
    assert payload["provider"] == "ollama"
    assert len(restored.entries) == len(index.entries)
    assert restored.entries[0].vector == index.entries[0].vector


def test_embedding_generator_reports_unsupported_provider_cleanly() -> None:
    generator = EmbeddingGenerator(FakeClient(provider="llamacpp"))
    assert generator.is_supported() is False
    with pytest.raises(AIGenerationError, match="does not support embeddings"):
        generator.generate_embedding("customer orders")


@patch("urllib.request.urlopen")
def test_embedding_generator_uses_current_ollama_embed_api(mock_urlopen: MagicMock) -> None:
    mock_urlopen.return_value = _make_mock_response({"embeddings": [[0.25, 0.5, 0.75]]})
    generator = EmbeddingGenerator(FakeClient(provider="ollama"))

    vector = generator.generate_embedding("story body text")

    assert vector == [0.25, 0.5, 0.75]
    request = mock_urlopen.call_args.args[0]
    assert request.full_url.endswith("/api/embed")
    assert json.loads(request.data.decode("utf-8")) == {
        "model": "mini-embed",
        "input": "story body text",
    }


@patch("urllib.request.urlopen")
def test_embedding_generator_accepts_legacy_ollama_embedding_shape(
    mock_urlopen: MagicMock,
) -> None:
    mock_urlopen.return_value = _make_mock_response({"embedding": [1, 2, 3]})
    generator = EmbeddingGenerator(FakeClient(provider="ollama"))

    assert generator.generate_embedding("legacy response") == [1.0, 2.0, 3.0]


def test_vector_search_load_rejects_incompatible_model(phase_tmp_dir: Path) -> None:
    generator = FakeEmbeddingGenerator({"minimal": [1.0, 0.0]}, provider="ollama")
    path = phase_tmp_dir / "atlas.embeddings"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "provider": "ollama",
                "model": "other-embed",
                "entries": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="model is incompatible"):
        VectorSearch.load(path, generator)


def test_vector_index_ignores_tables_without_semantic_material(phase_tmp_dir: Path) -> None:
    generator = FakeEmbeddingGenerator({"minimal": [1.0, 0.0]}, provider="ollama")
    index = VectorSearch(generator)
    result = build_phase10_result(phase_tmp_dir / "vector_material.db")
    target = result.get_table("main", "config_settings")
    assert target is not None
    target.semantic_short = None
    target.semantic_detailed = None
    target.semantic_domain = None
    target.semantic_role = None
    target.heuristic_type = None
    target.columns = []
    index.add_table(target)
    assert index.entries == []
