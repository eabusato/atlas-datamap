"""Public high-level SDK facade for programmatic Atlas workflows."""

from __future__ import annotations

import json
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from atlas.ai import (
    AIConfig,
    AIConnectionError,
    EmbeddingGenerator,
    LocalLLMClient,
    SemanticCache,
    SemanticEnricher,
    auto_detect_client,
)
from atlas.analysis import TableClassifier, TableScorer
from atlas.config import AtlasConnectionConfig
from atlas.connectors import BaseConnector, get_connector
from atlas.export import AtlasSnapshot, ScanArtifacts, sanitize_stem, save_artifacts
from atlas.introspection.runner import IntrospectionRunner, ProgressCallback
from atlas.search import AtlasQA, QACandidate, QAResult, VectorSearch
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.types import IntrospectionResult


@dataclass(slots=True, frozen=True)
class AtlasSigiloArtifact:
    """Immutable SVG artifact produced by the public Atlas facade."""

    result: IntrospectionResult
    svg_bytes: bytes
    style: str
    layout: str

    def save(self, output: str | Path) -> Path:
        """Persist the rendered SVG to a file path and return that path."""

        target = Path(output)
        if target.is_dir() or not target.suffix:
            target.mkdir(parents=True, exist_ok=True)
            target = target / f"{sanitize_stem(self.result.database)}.svg"
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.svg_bytes)
        return target

    def to_svg_text(self) -> str:
        """Return the SVG payload decoded as UTF-8 text."""

        return self.svg_bytes.decode("utf-8")


class Atlas:
    """High-level facade for Atlas scanning, rendering, enrichment, and QA."""

    def __init__(
        self,
        config: AtlasConnectionConfig,
        *,
        connector: BaseConnector | None = None,
    ) -> None:
        """Create a facade bound to one validated Atlas connection config."""

        self.config = config
        self._connector = connector or get_connector(config)

    @property
    def connector(self) -> BaseConnector:
        """Expose the resolved connector for advanced read-only integrations."""

        return self._connector

    def scan(
        self,
        *,
        on_progress: ProgressCallback | None = None,
        skip_columns: bool = False,
        skip_indexes: bool = False,
    ) -> IntrospectionResult:
        """Run full introspection and return the canonical metadata tree."""

        runner = IntrospectionRunner(
            self.config,
            self._connector,
            on_progress=on_progress,
            skip_columns=skip_columns,
            skip_indexes=skip_indexes,
        )
        return runner.run()

    def build_sigilo(
        self,
        result: IntrospectionResult,
        *,
        style: Literal["network", "seal", "compact"] = "network",
        layout: Literal["circular", "force"] = "circular",
        schema_filter: list[str] | None = None,
    ) -> AtlasSigiloArtifact:
        """Render a sigilo SVG artifact from an introspection result."""

        builder = DatamapSigiloBuilder.from_introspection_result(result)
        builder.set_style(style)
        builder.set_layout(layout)
        if schema_filter:
            builder.set_schema_filter(schema_filter)
        svg_bytes = builder.build()
        return AtlasSigiloArtifact(result=result, svg_bytes=svg_bytes, style=style, layout=layout)

    def save_scan_artifacts(
        self,
        result: IntrospectionResult,
        sigilo: AtlasSigiloArtifact,
        output_dir: str | Path,
        *,
        stem: str | None = None,
    ) -> ScanArtifacts:
        """Persist the canonical SVG, sigil JSON, and metadata JSON artifacts."""

        return save_artifacts(result, sigilo.svg_bytes, output_dir, stem=stem)

    def create_snapshot(
        self,
        result: IntrospectionResult,
        sigilo: AtlasSigiloArtifact,
        *,
        scores: list[dict[str, object]] | None = None,
        anomalies: list[dict[str, object]] | None = None,
        semantics: dict[str, object] | None = None,
    ) -> AtlasSnapshot:
        """Build a portable Atlas snapshot from a result and sigilo artifact."""

        return AtlasSnapshot.from_result(
            result,
            sigil_svg=sigilo.to_svg_text(),
            sigil_payload=json.dumps(
                result.to_dict(),
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            scores=scores,
            anomalies=anomalies,
            semantics=semantics,
        )

    def detect_local_llm(self, ai_config: AIConfig | None = None) -> LocalLLMClient:
        """Return the first reachable local LLM client or raise a stable error."""

        config = ai_config or AIConfig()
        try:
            return auto_detect_client(config)
        except AIConnectionError as exc:
            raise RuntimeError("No local LLM provider is reachable.") from exc

    def enrich(
        self,
        result: IntrospectionResult,
        *,
        ai_config: AIConfig | None = None,
        client: LocalLLMClient | None = None,
        cache: SemanticCache | None = None,
        tables_only: bool = False,
        parallel_workers: int = 4,
        force: bool = False,
    ) -> IntrospectionResult:
        """Apply semantic enrichment in-place and return the mutated result."""

        resolved_client = client or self.detect_local_llm(ai_config)
        resolved_cache = cache or SemanticCache(Path(".atlas_cache"))
        enricher = SemanticEnricher(resolved_client, cache=resolved_cache)
        session = nullcontext() if self._connector.is_connected else self._connector.session()
        with session:
            for schema in result.schemas:
                enricher.enrich_schema(
                    schema,
                    self._connector,
                    self.config.privacy_mode,
                    parallel_workers=parallel_workers,
                    force_recompute=force,
                    tables_only=tables_only,
                )
        resolved_cache.save()
        return result

    def ask(
        self,
        result: IntrospectionResult,
        question: str,
        *,
        ai_config: AIConfig | None = None,
        client: LocalLLMClient | None = None,
        embeddings_path: str | Path | None = None,
    ) -> QAResult:
        """Answer a natural-language question against an Atlas result."""

        prepared = IntrospectionResult.from_dict(result.to_dict())
        TableClassifier().classify_all(prepared)
        TableScorer(prepared).score_all()
        resolved_client = client or self.detect_local_llm(ai_config)
        qa = AtlasQA(prepared, resolved_client)
        answer = qa.ask(question)

        vector_index = self._load_vector_index(
            prepared,
            resolved_client,
            Path(embeddings_path) if embeddings_path is not None else None,
        )
        if vector_index is None or answer.candidates:
            return answer

        vector_candidates = vector_index.search(question)
        if not vector_candidates:
            return answer

        fallback_candidates: list[QACandidate] = []
        for candidate in vector_candidates[:5]:
            table = prepared.get_table(candidate.schema, candidate.table)
            if table is None:
                continue
            fallback_candidates.append(
                QACandidate(
                    schema=candidate.schema,
                    table=candidate.table,
                    final_score=max(0.0, candidate.similarity),
                    structural_score=0.0,
                    semantic_score=max(0.0, candidate.similarity),
                    heuristic_score=0.0,
                    reasoning="vector similarity fallback",
                    semantic_short=table.semantic_short,
                    semantic_domain=table.semantic_domain,
                    semantic_role=table.semantic_role,
                )
            )

        if not fallback_candidates:
            return answer

        return QAResult(
            question=answer.question,
            candidates=fallback_candidates,
            reasoning=f"{answer.reasoning} Vector similarity fallback supplied the final candidates.",
            suggested_query=answer.suggested_query,
            confidence=fallback_candidates[0].final_score,
        )

    def _load_vector_index(
        self,
        result: IntrospectionResult,
        client: LocalLLMClient,
        embeddings_path: Path | None,
    ) -> VectorSearch | None:
        if embeddings_path is None:
            return None
        generator = EmbeddingGenerator(client)
        if not generator.is_supported():
            return None
        if embeddings_path.exists():
            try:
                return VectorSearch.from_path(embeddings_path, generator)
            except Exception:
                pass
        index = VectorSearch(generator)
        try:
            index.build_from_result(result)
            index.save(embeddings_path)
        except Exception:
            return None
        return index


__all__ = ["Atlas", "AtlasSigiloArtifact"]
