"""Question answering and hybrid ranking over Atlas metadata."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from atlas.ai import LocalLLMClient, ResponseParser
from atlas.search.textual import AtlasSearch
from atlas.search.types import SearchResult
from atlas.types import IntrospectionResult, TableInfo

_NON_WORD_RE = re.compile(r"[\W_]+", re.UNICODE)
_MAX_RESULTS = 5
_QUESTION_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "do",
        "does",
        "for",
        "from",
        "how",
        "i",
        "in",
        "is",
        "look",
        "of",
        "on",
        "or",
        "related",
        "should",
        "show",
        "stored",
        "table",
        "tables",
        "the",
        "to",
        "tracked",
        "what",
        "where",
        "which",
    }
)
_INTERPRET_PROMPT = """You translate natural-language database questions into search vectors.

Question: "{question}"

Return valid JSON only. Do not reuse examples. Build search_terms from the user's own keywords.
If you are unsure, keep semantic_terms empty and suggested_query null.

Required schema:
{{
  "search_terms": ["keyword_from_question"],
  "semantic_terms": ["domain_or_synonym"],
  "reasoning": "One sentence grounded in the user's question.",
  "suggested_query": null
}}"""

_RERANK_PROMPT = """You compare candidate database tables for one user question.

Question: "{question}"

Candidates:
{candidates}

Return valid JSON only. Use only the candidate names provided above.
Prefer candidates whose semantic_short, semantic_detailed, role, domain, and column names clearly match the question.
If nothing is convincing, return an empty preferred_tables list.

Required schema:
{{
  "preferred_tables": ["schema.table"],
  "reasoning": "One sentence grounded in the candidate metadata."
}}"""


@dataclass(frozen=True, slots=True)
class QACandidate:
    """Ranked table candidate for a natural-language question."""

    schema: str
    table: str
    final_score: float
    structural_score: float
    semantic_score: float
    heuristic_score: float
    reasoning: str
    semantic_short: str | None
    semantic_domain: str | None
    semantic_role: str | None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table}"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "table": self.table,
            "qualified_name": self.qualified_name,
            "final_score": round(self.final_score, 4),
            "structural_score": round(self.structural_score, 4),
            "semantic_score": round(self.semantic_score, 4),
            "heuristic_score": round(self.heuristic_score, 4),
            "reasoning": self.reasoning,
            "semantic_short": self.semantic_short,
            "semantic_domain": self.semantic_domain,
            "semantic_role": self.semantic_role,
        }


@dataclass(slots=True)
class QAResult:
    """Natural-language answer synthesized from ranked table candidates."""

    question: str
    candidates: list[QACandidate]
    reasoning: str
    suggested_query: str | None
    confidence: float

    def to_dict(self) -> dict[str, object]:
        return {
            "question": self.question,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "reasoning": self.reasoning,
            "suggested_query": self.suggested_query,
            "confidence": round(self.confidence, 4),
        }


@dataclass(frozen=True, slots=True)
class _Interpretation:
    search_terms: list[str]
    semantic_terms: list[str]
    reasoning: str
    suggested_query: str | None
    llm_failed: bool = False


class AtlasQA:
    """Hybrid structural and semantic ranking for natural-language questions."""

    def __init__(
        self,
        result: IntrospectionResult,
        client: LocalLLMClient,
        search: AtlasSearch | None = None,
    ) -> None:
        self._result = result
        self._client = client
        self._search = search or AtlasSearch(result)

    def ask(self, question: str) -> QAResult:
        normalized_question = question.strip()
        if not normalized_question:
            raise ValueError("Question must not be empty.")

        interpretation = self._interpret_question(normalized_question)
        search_query = " ".join(interpretation.search_terms).strip()
        structural_hits = self._search.search_tables(search_query) if search_query else []
        structural_scores = self._normalize_structural_scores(structural_hits)
        semantic_terms = self._normalize_list(interpretation.semantic_terms)
        question_terms = self._tokenize(normalized_question, for_question=True)
        semantic_probe_terms = sorted(set(question_terms) | set(semantic_terms))
        candidate_tables = self._candidate_tables(structural_scores, semantic_probe_terms)

        candidates: list[QACandidate] = []
        for table in candidate_tables:
            structural_score = structural_scores.get(table.qualified_name, 0.0)
            semantic_score = self._semantic_score(table, semantic_probe_terms)
            heuristic_score = self._heuristic_score(table, question_terms, semantic_terms)
            final_score = self._clamp(
                structural_score * 0.25 + semantic_score * 0.55 + heuristic_score * 0.20
            )
            if final_score <= 0.0:
                continue
            candidates.append(
                QACandidate(
                    schema=table.schema,
                    table=table.name,
                    final_score=final_score,
                    structural_score=structural_score,
                    semantic_score=semantic_score,
                    heuristic_score=heuristic_score,
                    reasoning=self._candidate_reasoning(
                        table,
                        structural_score=structural_score,
                        semantic_score=semantic_score,
                        heuristic_score=heuristic_score,
                        structural_hits=structural_hits,
                    ),
                    semantic_short=table.semantic_short,
                    semantic_domain=table.semantic_domain,
                    semantic_role=table.semantic_role,
                )
            )

        candidates.sort(
            key=lambda item: (
                -item.final_score,
                -item.semantic_score,
                -item.structural_score,
                item.qualified_name,
            )
        )
        rerank_boosts = self._llm_rerank_boosts(normalized_question, candidates)
        if rerank_boosts:
            candidates.sort(
                key=lambda item: (
                    -(item.final_score + rerank_boosts.get(item.qualified_name, 0.0)),
                    -item.semantic_score,
                    -item.structural_score,
                    item.qualified_name,
                )
            )
        top_candidates = candidates[:_MAX_RESULTS]
        return QAResult(
            question=normalized_question,
            candidates=top_candidates,
            reasoning=interpretation.reasoning,
            suggested_query=interpretation.suggested_query,
            confidence=self._result_confidence(top_candidates, interpretation),
        )

    def _interpret_question(self, question: str) -> _Interpretation:
        prompt = _INTERPRET_PROMPT.format(question=question.replace('"', '\\"'))
        try:
            payload = ResponseParser.extract_json(self._client.generate(prompt))
            if not self._interpretation_is_grounded(question, payload):
                raise ValueError("Ungrounded interpretation")
        except Exception:
            fallback_terms = sorted(self._tokenize(question, for_question=True))
            return _Interpretation(
                search_terms=fallback_terms,
                semantic_terms=[],
                reasoning=(
                    "The local LLM could not interpret the question, so Atlas used only "
                    "structural tokens extracted from the question."
                ),
                suggested_query=None,
                llm_failed=True,
            )

        return _Interpretation(
            search_terms=self._normalize_list(payload.get("search_terms")),
            semantic_terms=self._normalize_list(payload.get("semantic_terms")),
            reasoning=self._normalize_reasoning(payload.get("reasoning")),
            suggested_query=self._optional_text(payload.get("suggested_query")),
            llm_failed=False,
        )

    def _interpretation_is_grounded(self, question: str, payload: dict[str, Any]) -> bool:
        question_tokens = self._tokenize(question, for_question=True)
        if not question_tokens:
            return True

        search_terms = set(self._normalize_list(payload.get("search_terms")))
        semantic_terms = set(self._normalize_list(payload.get("semantic_terms")))
        suggested_query_tokens = self._tokenize(self._optional_text(payload.get("suggested_query")))
        reasoning_tokens = self._tokenize(self._optional_text(payload.get("reasoning")))

        explicit_overlap = (search_terms | semantic_terms) & question_tokens
        contextual_overlap = (suggested_query_tokens | reasoning_tokens) & question_tokens
        return bool(explicit_overlap or contextual_overlap)

    def _normalize_reasoning(self, value: Any) -> str:
        text = self._optional_text(value)
        return text or "Atlas translated the question into structural and semantic search hints."

    def _candidate_tables(
        self,
        structural_scores: dict[str, float],
        semantic_terms: list[str],
    ) -> list[TableInfo]:
        tables: list[TableInfo] = []
        seen: set[str] = set()

        for table in self._result.all_tables():
            has_structural = table.qualified_name in structural_scores
            has_semantic = bool(semantic_terms) and self._semantic_score(table, semantic_terms) > 0.0
            if not has_structural and not has_semantic:
                continue
            if table.qualified_name in seen:
                continue
            seen.add(table.qualified_name)
            tables.append(table)
        return tables

    def _normalize_structural_scores(self, hits: list[SearchResult]) -> dict[str, float]:
        if not hits:
            return {}
        max_score = max(hit.score for hit in hits)
        if max_score <= 0.0:
            return {}
        return {
            hit.qualified_name: self._clamp(hit.score / max_score)
            for hit in hits
        }

    def _semantic_score(self, table: TableInfo, semantic_terms: list[str]) -> float:
        if not semantic_terms:
            return 0.0
        semantic_tokens = set(semantic_terms)
        table_tokens = self._semantic_tokens(table)
        if not table_tokens:
            return 0.0
        overlap_count = len(semantic_tokens & table_tokens)
        if overlap_count <= 0:
            return 0.0
        coverage = overlap_count / len(semantic_tokens)
        overlap_boost = min(0.3, 0.1 * overlap_count)
        return self._clamp((coverage + overlap_boost) * self._table_semantic_confidence(table))

    def _table_semantic_confidence(self, table: TableInfo) -> float:
        if table.semantic_confidence > 0.0:
            return self._clamp(table.semantic_confidence)
        column_confidences = [
            column.semantic_confidence
            for column in table.columns
            if column.semantic_confidence > 0.0
        ]
        if not column_confidences:
            return 0.0
        return self._clamp(sum(column_confidences) / len(column_confidences))

    def _semantic_tokens(self, table: TableInfo) -> set[str]:
        tokens: set[str] = set()
        for text in (
            table.semantic_short,
            table.semantic_detailed,
            table.semantic_domain,
            table.semantic_role,
            table.comment,
        ):
            tokens.update(self._tokenize(text))
        for column in table.columns:
            for text in (
                column.semantic_short,
                column.semantic_detailed,
                column.semantic_role,
                column.name,
            ):
                tokens.update(self._tokenize(text))
        return tokens

    def _heuristic_score(
        self,
        table: TableInfo,
        question_terms: set[str],
        semantic_terms: list[str],
    ) -> float:
        score = self._clamp(table.relevance_score)
        heuristic_tokens = self._tokenize(table.heuristic_type)
        semantic_token_set = set(semantic_terms)
        if heuristic_tokens & question_terms:
            score += 0.15
        if heuristic_tokens & semantic_token_set:
            score += 0.15
        if table.heuristic_confidence > 0.0:
            score *= max(0.5, self._clamp(table.heuristic_confidence))
        return self._clamp(score)

    def _candidate_reasoning(
        self,
        table: TableInfo,
        *,
        structural_score: float,
        semantic_score: float,
        heuristic_score: float,
        structural_hits: list[SearchResult],
    ) -> str:
        fragments = [
            f"structural={structural_score:.2f}",
            f"semantic={semantic_score:.2f}",
            f"heuristic={heuristic_score:.2f}",
        ]
        hit_reason = next(
            (
                hit.reason
                for hit in structural_hits
                if hit.qualified_name == table.qualified_name
            ),
            None,
        )
        if hit_reason is not None:
            fragments.append(f"search={hit_reason}")
        if table.semantic_domain:
            fragments.append(f"domain={table.semantic_domain}")
        if table.semantic_role:
            fragments.append(f"role={table.semantic_role}")
        return "; ".join(fragments)

    def _llm_rerank_boosts(
        self,
        question: str,
        candidates: list[QACandidate],
    ) -> dict[str, float]:
        shortlist = candidates[:8]
        if len(shortlist) < 2:
            return {}

        candidate_lookup = {candidate.qualified_name: candidate for candidate in shortlist}
        prompt = _RERANK_PROMPT.format(
            question=question.replace('"', '\\"'),
            candidates=self._format_rerank_candidates(shortlist),
        )
        try:
            payload = ResponseParser.extract_json(self._client.generate(prompt))
        except Exception:
            return {}

        preferred = payload.get("preferred_tables")
        if not isinstance(preferred, list):
            return {}

        boosts: dict[str, float] = {}
        rank_boosts = [0.12, 0.08, 0.05]
        applied = 0
        for item in preferred:
            if not isinstance(item, str):
                continue
            qualified_name = item.strip()
            if qualified_name not in candidate_lookup:
                continue
            if qualified_name in boosts:
                continue
            boost = rank_boosts[min(applied, len(rank_boosts) - 1)]
            boosts[qualified_name] = boost
            applied += 1
            if applied >= len(rank_boosts):
                break
        return boosts

    def _format_rerank_candidates(self, candidates: list[QACandidate]) -> str:
        lines: list[str] = []
        for candidate in candidates:
            table = self._result.get_table(candidate.schema, candidate.table)
            if table is None:
                continue
            column_names = ", ".join(column.name for column in table.columns[:12]) or "none"
            lines.append(
                f"- {candidate.qualified_name}: "
                f"short={table.semantic_short or 'unknown'}; "
                f"detailed={table.semantic_detailed or 'unknown'}; "
                f"domain={table.semantic_domain or 'unknown'}; "
                f"role={table.semantic_role or 'unknown'}; "
                f"columns={column_names}"
            )
        return "\n".join(lines) if lines else "- none"

    def _result_confidence(
        self,
        candidates: list[QACandidate],
        interpretation: _Interpretation,
    ) -> float:
        if not candidates:
            return 0.0
        confidence = candidates[0].final_score
        if interpretation.llm_failed:
            confidence *= 0.7
        if not interpretation.semantic_terms:
            confidence *= 0.85
        return self._clamp(confidence)

    @staticmethod
    def _tokenize(text: str | None, *, for_question: bool = False) -> set[str]:
        if text is None:
            return set()
        tokens: set[str] = set()
        for token in _NON_WORD_RE.sub(" ", text.casefold()).split():
            if len(token) <= 1:
                continue
            tokens.add(token)
            if len(token) > 3 and token.endswith("ies"):
                tokens.add(f"{token[:-3]}y")
            elif len(token) > 3 and token.endswith("es"):
                tokens.add(token[:-2])
            elif len(token) > 2 and token.endswith("s"):
                tokens.add(token[:-1])
        if for_question:
            tokens = {token for token in tokens if token not in _QUESTION_STOPWORDS}
        return tokens

    @classmethod
    def _normalize_list(cls, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        normalized: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str):
                continue
            for token in sorted(cls._tokenize(item)):
                if token not in seen:
                    seen.add(token)
                    normalized.append(token)
        return normalized

    @staticmethod
    def _optional_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _clamp(value: float) -> float:
        return max(0.0, min(1.0, value))
