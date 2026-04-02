"""Heuristic relevance scoring for Atlas tables."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field

from atlas.types import IntrospectionResult, TableInfo

_VOLUME_WEIGHT = 0.30
_CONNECTIVITY_WEIGHT = 0.30
_FILL_RATE_WEIGHT = 0.15
_INDEX_WEIGHT = 0.10
_NAME_WEIGHT = 0.10
_COMMENT_WEIGHT = 0.05
assert (
    _VOLUME_WEIGHT
    + _CONNECTIVITY_WEIGHT
    + _FILL_RATE_WEIGHT
    + _INDEX_WEIGHT
    + _NAME_WEIGHT
    + _COMMENT_WEIGHT
    == 1.0
)

_RE_STAGING_NAME = re.compile(r"(?i)(?:^(?:stg|tmp|temp|stage|stge)_|_(?:stg|tmp|temp|stage)$)")
_STAGING_TOKENS = ("stg", "tmp", "temp", "stage", "stge")


@dataclass(slots=True)
class ScoreBreakdown:
    """Weighted breakdown of table relevance factors."""

    volume_score: float = 0.0
    connectivity_score: float = 0.0
    fill_rate_score: float = 0.0
    index_score: float = 0.0
    name_score: float = 0.0
    comment_score: float = 0.0

    @property
    def total(self) -> float:
        weighted_total = (
            self.volume_score * _VOLUME_WEIGHT
            + self.connectivity_score * _CONNECTIVITY_WEIGHT
            + self.fill_rate_score * _FILL_RATE_WEIGHT
            + self.index_score * _INDEX_WEIGHT
            + self.name_score * _NAME_WEIGHT
            + self.comment_score * _COMMENT_WEIGHT
        )
        return round(weighted_total, 6)

    def to_dict(self) -> dict[str, float]:
        return {
            "volume_score": round(self.volume_score, 4),
            "connectivity_score": round(self.connectivity_score, 4),
            "fill_rate_score": round(self.fill_rate_score, 4),
            "index_score": round(self.index_score, 4),
            "name_score": round(self.name_score, 4),
            "comment_score": round(self.comment_score, 4),
            "total": round(self.total, 4),
        }


@dataclass(slots=True)
class TableScore:
    """Final table relevance score and ranking."""

    table: str
    schema: str
    score: float
    breakdown: ScoreBreakdown = field(default_factory=ScoreBreakdown)
    rank: int = 0

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table}"

    def to_dict(self) -> dict[str, object]:
        return {
            "table": self.table,
            "schema": self.schema,
            "qualified_name": self.qualified_name,
            "score": round(self.score, 4),
            "breakdown": self.breakdown.to_dict(),
            "rank": self.rank,
        }


def _score_volume(row_count: int) -> float:
    if row_count <= 0:
        return 0.0
    if row_count < 100:
        return 0.05
    if row_count < 1_000:
        return 0.10
    if row_count < 10_000:
        return 0.30
    if row_count < 100_000:
        return 0.55
    if row_count < 1_000_000:
        return 0.75
    return 1.0


def _score_connectivity(fk_in_degree: int, fk_out_degree: int) -> float:
    return min(1.0, (fk_in_degree + fk_out_degree) / 10.0)


def _score_fill_rate(table: TableInfo) -> float:
    if not table.columns:
        return 0.5
    scores: list[float] = []
    for column in table.columns:
        if column.stats.row_count > 0:
            scores.append(column.stats.fill_rate)
        else:
            scores.append(0.5 if column.is_nullable else 1.0)
    return sum(scores) / len(scores)


def _score_indexes(table: TableInfo) -> float:
    if not table.indexes:
        return 0.0
    non_primary_indexes = [index for index in table.indexes if not index.is_primary]
    if not non_primary_indexes:
        return 0.3
    if len(non_primary_indexes) >= 2 and any(index.is_unique for index in non_primary_indexes):
        return 1.0
    return 0.6


def _score_name(table_name: str) -> float:
    lowered = table_name.lower()
    if _RE_STAGING_NAME.search(lowered):
        return 0.0
    if any(token in lowered for token in _STAGING_TOKENS):
        return 0.3
    return 1.0


def _score_comment(comment: str | None) -> float:
    return 1.0 if comment is not None and comment.strip() else 0.0


class TableScorer:
    """Assign weighted relevance to Atlas introspection results."""

    def __init__(self, result: IntrospectionResult) -> None:
        self._result = result

    def score_table(self, table: TableInfo) -> TableScore:
        breakdown = ScoreBreakdown(
            volume_score=_score_volume(table.row_count_estimate),
            connectivity_score=_score_connectivity(table.fk_in_degree, len(table.foreign_keys)),
            fill_rate_score=_score_fill_rate(table),
            index_score=_score_indexes(table),
            name_score=_score_name(table.name),
            comment_score=_score_comment(table.comment),
        )
        return TableScore(
            table=table.name,
            schema=table.schema,
            score=breakdown.total,
            breakdown=breakdown,
        )

    def score_all(self, schema: str | None = None) -> list[TableScore]:
        scores: list[TableScore] = []
        for table in self._result.all_tables():
            if schema is not None and table.schema != schema:
                continue
            score = self.score_table(table)
            table.relevance_score = score.score
            scores.append(score)

        scores.sort(key=lambda item: (-item.score, item.schema, item.table))
        for index, score in enumerate(scores, start=1):
            score.rank = index
        return scores

    def get_top_tables(self, n: int, schema: str | None = None) -> list[TableScore]:
        if n <= 0:
            return []
        return self.score_all(schema=schema)[:n]

    def get_tables_by_domain_cluster(self) -> dict[str, list[TableScore]]:
        grouped: dict[str, list[TableScore]] = defaultdict(list)
        table_by_name = {
            table.qualified_name: table
            for table in self._result.all_tables()
        }
        for score in self.score_all():
            table = table_by_name[score.qualified_name]
            cluster = table.heuristic_type or "unknown"
            grouped[cluster].append(score)
        return {
            cluster: sorted(items, key=lambda item: (-item.score, item.schema, item.table))
            for cluster, items in grouped.items()
            if items
        }
