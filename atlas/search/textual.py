"""In-memory textual search engine over Atlas metadata."""

from __future__ import annotations

import re

from atlas.search.types import EntityType, SearchResult
from atlas.types import AtlasType, IntrospectionResult

_SCORE_L0_EXACT_SET = 20.0
_SCORE_L1_EXACT_TOKEN = 10.0
_SCORE_L2_SUBSTR = 5.0
_SCORE_L3_TYPE = 4.0
_SCORE_L4_COMMENT = 2.0

_RE_CAMEL = re.compile(r"([a-z0-9])([A-Z])")
_RE_NON_WORD = re.compile(r"[\W_]+", re.UNICODE)
_ENTITY_SORT = {
    EntityType.TABLE: 0,
    EntityType.COLUMN: 1,
    EntityType.SCHEMA: 2,
}


class AtlasSearch:
    """Pure in-memory search over schemas, tables, and columns."""

    def __init__(self, result: IntrospectionResult) -> None:
        self._result = result

    def _normalize_tokens(self, text: str | None) -> set[str]:
        """Normalize a free-form string into search tokens."""

        if text is None:
            return set()
        expanded = _RE_CAMEL.sub(r"\1 \2", text)
        normalized = _RE_NON_WORD.sub(" ", expanded.casefold())
        return {token for token in normalized.split() if len(token) > 1}

    def _calculate_match_score(
        self,
        query_tokens: set[str],
        target_name: str,
        target_comment: str | None,
        target_type: str | None = None,
    ) -> tuple[float, str | None]:
        """Return score and ranking explanation for a single target."""

        if not query_tokens:
            return 0.0, None

        name_tokens = self._normalize_tokens(target_name)
        comment_tokens = self._normalize_tokens(target_comment)
        type_tokens = self._normalize_tokens(target_type)
        lowered_name = target_name.casefold()

        score = 0.0
        reasons: list[str] = []

        if name_tokens and query_tokens == name_tokens:
            score += _SCORE_L0_EXACT_SET
            reasons.append("L0 exact name token-set")

        for token in sorted(query_tokens):
            if token in name_tokens:
                score += _SCORE_L1_EXACT_TOKEN
                reasons.append(f"L1 exact name token:{token}")
            elif token in lowered_name:
                score += _SCORE_L2_SUBSTR
                reasons.append(f"L2 name substring:{token}")

            if token in type_tokens:
                score += _SCORE_L3_TYPE
                reasons.append(f"L3 type hint:{token}")

            if token in comment_tokens:
                score += _SCORE_L4_COMMENT
                reasons.append(f"L4 comment token:{token}")

        if score <= 0.0:
            return 0.0, None
        return score, "; ".join(reasons)

    def search_tables(
        self,
        query: str,
        schema_filter: str | None = None,
        type_filter: str | None = None,
    ) -> list[SearchResult]:
        """Search tables by name, comment, or heuristic type."""

        query_tokens = self._normalize_tokens(query)
        wanted_type = type_filter.casefold() if type_filter is not None else None
        results: list[SearchResult] = []

        for table in self._result.all_tables():
            if schema_filter is not None and table.schema != schema_filter:
                continue
            heuristic_type = (table.heuristic_type or "").casefold()
            if wanted_type is not None and heuristic_type != wanted_type:
                continue
            score, reason = self._calculate_match_score(
                query_tokens,
                table.name,
                table.comment,
                table.heuristic_type,
            )
            if reason is None:
                continue
            results.append(
                SearchResult(
                    entity_type=EntityType.TABLE,
                    schema=table.schema,
                    table=table.name,
                    column=None,
                    score=score,
                    reason=reason,
                )
            )
        return self._sort_results(results)

    def search_columns(
        self,
        query: str,
        schema_filter: str | None = None,
    ) -> list[SearchResult]:
        """Search columns by name, comment, or canonical type."""

        query_tokens = self._normalize_tokens(query)
        results: list[SearchResult] = []

        for table in self._result.all_tables():
            if schema_filter is not None and table.schema != schema_filter:
                continue
            for column in table.columns:
                canonical_type = (column.canonical_type or AtlasType.UNKNOWN).value
                score, reason = self._calculate_match_score(
                    query_tokens,
                    column.name,
                    column.comment,
                    canonical_type,
                )
                if reason is None:
                    continue
                results.append(
                    SearchResult(
                        entity_type=EntityType.COLUMN,
                        schema=table.schema,
                        table=table.name,
                        column=column.name,
                        score=score,
                        reason=reason,
                    )
                )
        return self._sort_results(results)

    def search_schema(self, query: str) -> list[SearchResult]:
        """Search schemas, tables, and columns in one merged ranked list."""

        query_tokens = self._normalize_tokens(query)
        results = self._search_schemas_only(query_tokens)
        results.extend(self.search_tables(query))
        results.extend(self.search_columns(query))
        return self._sort_results(results)

    def _search_schemas_only(self, query_tokens: set[str]) -> list[SearchResult]:
        """Search schema names only."""

        results: list[SearchResult] = []
        for schema in self._result.schemas:
            score, reason = self._calculate_match_score(
                query_tokens,
                schema.name,
                None,
                None,
            )
            if reason is None:
                continue
            results.append(
                SearchResult(
                    entity_type=EntityType.SCHEMA,
                    schema=schema.name,
                    table=None,
                    column=None,
                    score=score,
                    reason=reason,
                )
            )
        return results

    def _sort_results(self, results: list[SearchResult]) -> list[SearchResult]:
        """Return a stable deterministic ordering for search hits."""

        return sorted(
            results,
            key=lambda item: (
                -item.score,
                _ENTITY_SORT[item.entity_type],
                item.schema,
                item.table or "",
                item.column or "",
            ),
        )
