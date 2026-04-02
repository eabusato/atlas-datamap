"""Canonical types for the Atlas search subsystem."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class EntityType(StrEnum):
    """Kind of database object returned by a search query."""

    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"


@dataclass(frozen=True, slots=True)
class SearchResult:
    """A single search hit with relevance metadata."""

    entity_type: EntityType
    schema: str
    table: str | None
    column: str | None
    score: float
    reason: str

    @property
    def qualified_name(self) -> str:
        """Return the fully qualified identifier for the matched entity."""

        if self.entity_type is EntityType.SCHEMA:
            return self.schema
        if self.entity_type is EntityType.TABLE:
            return f"{self.schema}.{self.table}"
        return f"{self.schema}.{self.table}.{self.column}"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the hit for JSON or CLI formatting."""

        return {
            "entity_type": self.entity_type.value,
            "schema": self.schema,
            "table": self.table,
            "column": self.column,
            "score": round(self.score, 4),
            "reason": self.reason,
            "qualified_name": self.qualified_name,
        }
