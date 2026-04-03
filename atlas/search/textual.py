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
_SCORE_L5_SEMANTIC = 4.0
_SCORE_L6_COLUMN = 2.0

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

    def _normalize_tokens(self, text: str | None, *, expand_inflections: bool = False) -> set[str]:
        """Normalize a free-form string into search tokens."""

        if text is None:
            return set()
        expanded = _RE_CAMEL.sub(r"\1 \2", text)
        normalized = _RE_NON_WORD.sub(" ", expanded.casefold())
        tokens: set[str] = set()
        for token in normalized.split():
            if len(token) <= 1:
                continue
            if expand_inflections:
                tokens.update(self._expand_token_forms(token))
            else:
                tokens.add(token)
        return tokens

    @staticmethod
    def _expand_token_forms(token: str) -> set[str]:
        forms = {token}
        if len(token) > 3 and token.endswith("ies"):
            forms.add(f"{token[:-3]}y")
        elif len(token) > 3 and token.endswith("es"):
            forms.add(token[:-2])
        elif len(token) > 2 and token.endswith("s"):
            forms.add(token[:-1])
        return {item for item in forms if len(item) > 1}

    def _calculate_match_score(
        self,
        query_tokens: set[str],
        raw_query_tokens: set[str],
        target_name: str,
        exact_name: str | None,
        target_comment: str | None,
        target_type: str | None = None,
        semantic_text: str | None = None,
        column_text: str | None = None,
    ) -> tuple[float, str | None]:
        """Return score and ranking explanation for a single target."""

        if not query_tokens:
            return 0.0, None

        exact_name_tokens = self._normalize_tokens(exact_name or target_name)
        name_tokens = self._normalize_tokens(target_name)
        comment_tokens = self._normalize_tokens(target_comment, expand_inflections=True)
        type_tokens = self._normalize_tokens(target_type, expand_inflections=True)
        semantic_tokens = self._normalize_tokens(semantic_text, expand_inflections=True)
        column_tokens = self._normalize_tokens(column_text, expand_inflections=True)
        lowered_name = target_name.casefold()

        score = 0.0
        reasons: list[str] = []

        if exact_name_tokens and raw_query_tokens == exact_name_tokens:
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

            if token in semantic_tokens:
                score += _SCORE_L5_SEMANTIC
                reasons.append(f"L5 semantic token:{token}")

            if token in column_tokens:
                score += _SCORE_L6_COLUMN
                reasons.append(f"L6 column token:{token}")

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

        raw_query_tokens = self._normalize_tokens(query)
        query_tokens = self._normalize_tokens(query, expand_inflections=True)
        wanted_type = type_filter.casefold() if type_filter is not None else None
        results: list[SearchResult] = []

        for table in self._result.all_tables():
            if schema_filter is not None and table.schema != schema_filter:
                continue
            heuristic_type = (table.heuristic_type or "").casefold()
            if wanted_type is not None and heuristic_type != wanted_type:
                continue
            semantic_text = " ".join(
                value
                for value in (
                    table.semantic_short,
                    table.semantic_detailed,
                    table.semantic_domain,
                    table.semantic_role,
                )
                if value
            )
            column_text = " ".join(
                value
                for column in table.columns
                for value in (
                    column.name,
                    column.comment,
                    column.semantic_short,
                    column.semantic_detailed,
                    column.semantic_role,
                )
                if value
            )
            score, reason = self._calculate_match_score(
                query_tokens,
                raw_query_tokens,
                f"{table.schema} {table.name} {table.qualified_name}",
                table.name,
                table.comment,
                table.heuristic_type,
                semantic_text,
                column_text,
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

        raw_query_tokens = self._normalize_tokens(query)
        query_tokens = self._normalize_tokens(query, expand_inflections=True)
        results: list[SearchResult] = []

        for table in self._result.all_tables():
            if schema_filter is not None and table.schema != schema_filter:
                continue
            for column in table.columns:
                canonical_type = (column.canonical_type or AtlasType.UNKNOWN).value
                score, reason = self._calculate_match_score(
                    query_tokens,
                    raw_query_tokens,
                    column.name,
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

        query_tokens = self._normalize_tokens(query, expand_inflections=True)
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
                query_tokens,
                schema.name,
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
