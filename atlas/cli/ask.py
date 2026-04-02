"""Command implementation for ``atlas ask``."""

from __future__ import annotations

import json
from pathlib import Path

import click

from atlas.ai import AIConfig, AIConfigError, EmbeddingGenerator, LocalLLMClient, build_client
from atlas.analysis import TableClassifier, TableScorer
from atlas.cli._common import require_existing_path, resolve_config
from atlas.connectors import get_connector
from atlas.search import AtlasQA, QAResult, VectorCandidate, VectorSearch
from atlas.types import IntrospectionResult

_INTERACTIVE_HISTORY_LIMIT = 3


def _load_sigil_result(sigil_path: Path) -> IntrospectionResult:
    payload = sigil_path.read_text(encoding="utf-8")
    return IntrospectionResult.from_json(payload)


def _load_result_from_connection(db: str | None, config_path: Path | None) -> IntrospectionResult:
    config = resolve_config(db=db, config_path=str(config_path) if config_path else None)
    connector = get_connector(config)
    with connector.session():
        return connector.introspect_all()


def _prepare_result(result: IntrospectionResult) -> IntrospectionResult:
    TableClassifier().classify_all(result)
    TableScorer(result).score_all()
    return result


def _load_ai_client(config_path: Path | None) -> LocalLLMClient:
    try:
        ai_config = AIConfig.from_file(config_path) if config_path else AIConfig()
        client = build_client(ai_config)
    except AIConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:
        raise click.ClickException(f"Failed to initialize local AI client: {exc}") from exc

    if not client.is_available():
        raise click.ClickException(
            "Local AI provider is unavailable. Start the configured local server and retry."
        )
    return client


def _resolve_structural_source(
    sigil_path: Path | None,
    db: str | None,
    config_path: Path | None,
) -> tuple[str, Path | None]:
    """Resolve which input supplies the structural metadata graph."""

    if sigil_path is not None:
        if db is not None or config_path is not None:
            raise click.UsageError(
                "Provide exactly one structural source: --sigil, --db, or --config."
            )
        return "sigil", sigil_path
    if db is not None:
        return "db", config_path
    if config_path is not None:
        return "config", config_path
    raise click.UsageError("Provide exactly one structural source: --sigil, --db, or --config.")


def _embeddings_path(sigil_path: Path | None) -> Path | None:
    if sigil_path is None:
        return None
    return sigil_path.with_suffix(".embeddings")


def _load_vector_index(
    result: IntrospectionResult,
    sigil_path: Path | None,
    client: LocalLLMClient,
    *,
    no_embeddings: bool,
) -> VectorSearch | None:
    if no_embeddings:
        return None

    generator = EmbeddingGenerator(client)
    if not generator.is_supported():
        return None

    sidecar_path = _embeddings_path(sigil_path)
    if sidecar_path is not None and sidecar_path.exists():
        try:
            return VectorSearch.load(sidecar_path, generator)
        except Exception as exc:
            click.echo(f"[atlas ask] Ignoring incompatible embeddings cache: {exc}", err=True)

    try:
        index = VectorSearch(generator)
        index.build_from_result(result)
        if sidecar_path is not None:
            index.save(sidecar_path)
        return index
    except Exception as exc:
        click.echo(f"[atlas ask] Embeddings unavailable, continuing without vector search: {exc}", err=True)
        return None


def _render_text(
    answer: QAResult,
    vector_candidates: list[VectorCandidate],
) -> str:
    lines = [
        f"Question: {answer.question}",
        f"Reasoning: {answer.reasoning}",
        f"Confidence: {answer.confidence:.2f}",
    ]
    if answer.suggested_query:
        lines.append(f"Suggested SQL: {answer.suggested_query}")
    lines.append("Candidates:")
    if not answer.candidates:
        lines.append("- none")
    else:
        for candidate in answer.candidates:
            lines.append(
                f"- {candidate.qualified_name} "
                f"(score={candidate.final_score:.2f}, "
                f"structural={candidate.structural_score:.2f}, "
                f"semantic={candidate.semantic_score:.2f}, "
                f"heuristic={candidate.heuristic_score:.2f})"
            )
    if vector_candidates:
        lines.append("Vector Candidates:")
        for vector_candidate in vector_candidates:
            lines.append(
                f"- {vector_candidate.qualified_name} "
                f"(similarity={vector_candidate.similarity:.2f})"
            )
    return "\n".join(lines)


def _render_json(
    answer: QAResult,
    vector_candidates: list[VectorCandidate],
) -> str:
    payload = answer.to_dict()
    if vector_candidates:
        payload["vector_candidates"] = [candidate.to_dict() for candidate in vector_candidates]
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _contextualize_question(question: str, history: list[tuple[str, list[str]]]) -> str:
    if not history:
        return question
    recent = " | ".join(
        f"Q: {previous_question}; A: {', '.join(candidates) if candidates else 'none'}"
        for previous_question, candidates in history[-_INTERACTIVE_HISTORY_LIMIT:]
    )
    return f"Recent context: {recent}\nCurrent question: {question}"


def _run_question(
    qa: AtlasQA,
    question: str,
    history: list[tuple[str, list[str]]],
    vector_index: VectorSearch | None,
) -> tuple[QAResult, list[VectorCandidate]]:
    contextual_question = _contextualize_question(question, history)
    answer = qa.ask(contextual_question)
    answer.question = question
    vector_candidates = vector_index.search(contextual_question) if vector_index is not None else []
    history.append((question, [candidate.qualified_name for candidate in answer.candidates[:3]]))
    if len(history) > _INTERACTIVE_HISTORY_LIMIT:
        del history[:-_INTERACTIVE_HISTORY_LIMIT]
    return answer, vector_candidates


def _emit_answer(output_format: str, answer: QAResult, vector_candidates: list[VectorCandidate]) -> None:
    if output_format == "json":
        click.echo(_render_json(answer, vector_candidates))
    else:
        click.echo(_render_text(answer, vector_candidates))


@click.command("ask")
@click.option("--sigil", "sigil_path", type=click.Path(path_type=Path), required=False)
@click.option("--db", type=str, required=False, help="Database connection URL.")
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path),
    required=False,
    help="Path to atlas.toml used as the structural source when --db/--sigil are not provided.",
)
@click.option(
    "--ai-config",
    "ai_config_path",
    type=click.Path(path_type=Path),
    required=False,
    help="Path to atlas.toml used only for AI provider configuration.",
)
@click.option("--interactive", is_flag=True, help="Run an interactive question loop.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option("--no-embeddings", is_flag=True, help="Disable vector search even when supported.")
@click.argument("question", required=False)
def ask_cmd(
    sigil_path: Path | None,
    db: str | None,
    config_path: Path | None,
    ai_config_path: Path | None,
    interactive: bool,
    output_format: str,
    no_embeddings: bool,
    question: str | None,
) -> None:
    """Answer natural-language questions about Atlas metadata."""

    if not interactive and (question is None or not question.strip()):
        raise click.UsageError("Provide a question or use --interactive.")

    resolved_sigil = require_existing_path(sigil_path, kind="file") if sigil_path else None
    resolved_config = require_existing_path(config_path, kind="file") if config_path else None
    resolved_ai_config = (
        require_existing_path(ai_config_path, kind="file") if ai_config_path else None
    )
    structural_source, structural_config = _resolve_structural_source(
        resolved_sigil,
        db,
        resolved_config,
    )

    if structural_source == "sigil":
        assert resolved_sigil is not None
        result = _load_sigil_result(resolved_sigil)
    else:
        result = _load_result_from_connection(
            db,
            structural_config,
        )
    result = _prepare_result(result)

    ai_config_for_client = resolved_ai_config if resolved_ai_config is not None else resolved_config
    client = _load_ai_client(ai_config_for_client)
    if output_format == "text":
        model_info = client.get_model_info()
        click.echo(
            f"[atlas ask] Using local model {model_info.model_name} via {model_info.provider_name}.",
            err=True,
        )

    qa = AtlasQA(result, client)
    vector_index = _load_vector_index(
        result,
        resolved_sigil,
        client,
        no_embeddings=no_embeddings,
    )

    history: list[tuple[str, list[str]]] = []
    if question is not None and question.strip():
        answer, vector_candidates = _run_question(qa, question.strip(), history, vector_index)
        _emit_answer(output_format, answer, vector_candidates)
        if not interactive:
            return

    if not interactive:
        return

    while True:
        try:
            next_question = click.prompt("atlas ask", prompt_suffix=" > ").strip()
        except (EOFError, KeyboardInterrupt):
            click.echo("", err=output_format == "json")
            break
        if next_question.casefold() in {"exit", "quit"}:
            break
        if not next_question:
            continue
        answer, vector_candidates = _run_question(qa, next_question, history, vector_index)
        _emit_answer(output_format, answer, vector_candidates)
