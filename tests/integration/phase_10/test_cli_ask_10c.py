"""Integration tests for Phase 10C ask CLI."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from atlas.ai import AIConfig
from atlas.cli.ask import ask_cmd
from atlas.search import QACandidate, QAResult, VectorCandidate
from tests.integration.phase_10.helpers import build_phase10_result

pytestmark = [pytest.mark.integration, pytest.mark.phase_10c]


class FakeClient:
    def __init__(self) -> None:
        self.config = AIConfig(provider="ollama", model="llama3", base_url="http://localhost:11434")

    def is_available(self) -> bool:
        return True

    def get_model_info(self):
        class _Info:
            model_name = "llama3"
            provider_name = "ollama"

        return _Info()


class FakeQA:
    def __init__(self, result, client) -> None:
        del result, client
        self.questions: list[str] = []

    def ask(self, question: str) -> QAResult:
        self.questions.append(question)
        return QAResult(
            question=question,
            candidates=[
                QACandidate(
                    schema="main",
                    table="fact_orders",
                    final_score=0.91,
                    structural_score=0.88,
                    semantic_score=0.94,
                    heuristic_score=0.7,
                    reasoning="structural=0.88; semantic=0.94; heuristic=0.70",
                    semantic_short="Customer orders",
                    semantic_domain="sales",
                    semantic_role="transaction_header",
                )
            ],
            reasoning="The question points to the transactional order header table.",
            suggested_query="SELECT * FROM main.fact_orders LIMIT 20",
            confidence=0.91,
        )


class FakeVectorSearch:
    def __init__(self, generator) -> None:
        del generator

    def build_from_result(self, result) -> None:
        del result

    def save(self, path: Path) -> None:
        path.write_text('{"version": 1, "entries": []}', encoding="utf-8")

    @classmethod
    def load(cls, path: Path, generator):
        del path
        return cls(generator)

    def search(self, query: str, top_k: int = 5) -> list[VectorCandidate]:
        del query, top_k
        return [
            VectorCandidate(
                schema="main",
                table="log_payment_history",
                similarity=0.84,
                source_text="Table: main.log_payment_history.",
            )
        ]


class FakeEmbeddingGenerator:
    def __init__(self, client) -> None:
        self.client = client
        self.provider_name = "ollama"
        self.model_name = "llama3"

    def is_supported(self) -> bool:
        return True

    def generate_embedding(self, text: str) -> list[float]:
        del text
        return [1.0, 0.0]


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _write_sigil(path: Path) -> None:
    result = build_phase10_result(path.with_suffix(".db"))
    path.write_text(result.to_json(indent=2), encoding="utf-8")


def test_ask_single_shot_text_with_sigil(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    _write_sigil(sigil_path)
    monkeypatch.setattr("atlas.cli.ask.build_client", lambda config: FakeClient())
    monkeypatch.setattr("atlas.cli.ask.AtlasQA", FakeQA)
    monkeypatch.setattr("atlas.cli.ask.EmbeddingGenerator", FakeEmbeddingGenerator)
    monkeypatch.setattr("atlas.cli.ask.VectorSearch", FakeVectorSearch)

    result = runner.invoke(
        ask_cmd,
        ["--sigil", str(sigil_path), "where are customer orders stored?"],
    )

    assert result.exit_code == 0, result.output
    assert "Question: where are customer orders stored?" in result.output
    assert "main.fact_orders" in result.output
    assert "Vector Candidates:" in result.output


def test_ask_single_shot_json_is_parseable(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas_json.sigil"
    _write_sigil(sigil_path)
    monkeypatch.setattr("atlas.cli.ask.build_client", lambda config: FakeClient())
    monkeypatch.setattr("atlas.cli.ask.AtlasQA", FakeQA)
    monkeypatch.setattr("atlas.cli.ask.EmbeddingGenerator", FakeEmbeddingGenerator)
    monkeypatch.setattr("atlas.cli.ask.VectorSearch", FakeVectorSearch)

    result = runner.invoke(
        ask_cmd,
        ["--sigil", str(sigil_path), "--format", "json", "where is the product catalog?"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["question"] == "where is the product catalog?"
    assert payload["candidates"][0]["qualified_name"] == "main.fact_orders"
    assert payload["vector_candidates"][0]["qualified_name"] == "main.log_payment_history"


def test_ask_errors_when_no_question_and_not_interactive(
    runner: CliRunner,
    phase_tmp_dir: Path,
) -> None:
    sigil_path = phase_tmp_dir / "atlas_missing_question.sigil"
    _write_sigil(sigil_path)

    result = runner.invoke(ask_cmd, ["--sigil", str(sigil_path)])

    assert result.exit_code != 0
    assert "Provide a question or use --interactive." in result.output


def test_ask_errors_when_no_structural_source(runner: CliRunner) -> None:
    result = runner.invoke(ask_cmd, ["where are customer orders stored?"])
    assert result.exit_code != 0
    assert "Provide exactly one structural source" in result.output


def test_ask_interactive_mode_handles_multiple_questions_and_quit(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas_interactive.sigil"
    _write_sigil(sigil_path)
    monkeypatch.setattr("atlas.cli.ask.build_client", lambda config: FakeClient())
    monkeypatch.setattr("atlas.cli.ask.AtlasQA", FakeQA)
    monkeypatch.setattr("atlas.cli.ask.EmbeddingGenerator", FakeEmbeddingGenerator)
    monkeypatch.setattr("atlas.cli.ask.VectorSearch", FakeVectorSearch)

    result = runner.invoke(
        ask_cmd,
        ["--interactive", "--sigil", str(sigil_path)],
        input="where are customer orders stored?\nquit\n",
    )

    assert result.exit_code == 0, result.output
    assert "atlas ask >" in result.output
    assert "main.fact_orders" in result.output


def test_ask_degrades_cleanly_when_embeddings_are_disabled(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas_no_vector.sigil"
    _write_sigil(sigil_path)
    monkeypatch.setattr("atlas.cli.ask.build_client", lambda config: FakeClient())
    monkeypatch.setattr("atlas.cli.ask.AtlasQA", FakeQA)

    result = runner.invoke(
        ask_cmd,
        ["--sigil", str(sigil_path), "--no-embeddings", "where are customer orders stored?"],
    )

    assert result.exit_code == 0, result.output
    assert "main.fact_orders" in result.output
    assert "Vector Candidates:" not in result.output


def test_ask_can_initialize_from_config_source(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = phase_tmp_dir / "atlas.toml"
    config_path.write_text(
        """
[connection]
engine = "sqlite"
database = ":memory:"

[ai]
provider = "ollama"
model = "llama3"
base_url = "http://localhost:11434"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr("atlas.cli.ask.build_client", lambda config: FakeClient())
    monkeypatch.setattr("atlas.cli.ask.AtlasQA", FakeQA)
    monkeypatch.setattr("atlas.cli.ask._load_result_from_connection", lambda db, config_path: build_phase10_result(phase_tmp_dir / "from_config.db"))
    monkeypatch.setattr("atlas.cli.ask._load_vector_index", lambda result, sigil_path, client, no_embeddings: None)

    result = runner.invoke(
        ask_cmd,
        ["--config", str(config_path), "where are customer orders stored?"],
    )

    assert result.exit_code == 0, result.output
    assert "main.fact_orders" in result.output


def test_ask_accepts_ai_config_alongside_sigil_source(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas_semantic.sigil"
    _write_sigil(sigil_path)
    ai_config_path = phase_tmp_dir / "atlas.ai.toml"
    ai_config_path.write_text(
        """
[ai]
provider = "ollama"
model = "qwen2.5:1.5b"
base_url = "http://localhost:11434"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr("atlas.cli.ask.build_client", lambda config: FakeClient())
    monkeypatch.setattr("atlas.cli.ask.AtlasQA", FakeQA)
    monkeypatch.setattr("atlas.cli.ask.EmbeddingGenerator", FakeEmbeddingGenerator)
    monkeypatch.setattr("atlas.cli.ask.VectorSearch", FakeVectorSearch)

    result = runner.invoke(
        ask_cmd,
        [
            "--sigil",
            str(sigil_path),
            "--ai-config",
            str(ai_config_path),
            "where are customer orders stored?",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "main.fact_orders" in result.output


def test_ask_rejects_ambiguous_config_alongside_sigil(
    runner: CliRunner,
    phase_tmp_dir: Path,
) -> None:
    sigil_path = phase_tmp_dir / "atlas_ambiguous.sigil"
    _write_sigil(sigil_path)
    config_path = phase_tmp_dir / "atlas.toml"
    config_path.write_text(
        """
[connection]
engine = "sqlite"
database = ":memory:"
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        ask_cmd,
        ["--sigil", str(sigil_path), "--config", str(config_path), "where are customer orders stored?"],
    )

    assert result.exit_code != 0
    assert "Provide exactly one structural source" in result.output
