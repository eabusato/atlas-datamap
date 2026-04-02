"""Vector search over semantically enriched Atlas metadata."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

from atlas.ai.embeddings import EmbeddingGenerator
from atlas.types import IntrospectionResult, TableInfo

_VECTOR_FORMAT_VERSION = 1


@dataclass(frozen=True, slots=True)
class VectorIndexEntry:
    """One indexed semantic document for a table."""

    schema: str
    table: str
    vector: list[float]
    source_text: str

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table}"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "table": self.table,
            "qualified_name": self.qualified_name,
            "vector": [float(value) for value in self.vector],
            "source_text": self.source_text,
        }


@dataclass(frozen=True, slots=True)
class VectorCandidate:
    """Ranked semantic match from vector search."""

    schema: str
    table: str
    similarity: float
    source_text: str

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table}"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "table": self.table,
            "qualified_name": self.qualified_name,
            "similarity": round(self.similarity, 4),
            "source_text": self.source_text,
        }


class VectorSearch:
    """Pure-Python cosine similarity search over semantic table documents."""

    def __init__(self, generator: EmbeddingGenerator) -> None:
        self.generator = generator
        self.entries: list[VectorIndexEntry] = []

    def add_table(self, table: TableInfo) -> None:
        source_text = self._build_source_text(table)
        if source_text is None:
            return
        vector = self.generator.generate_embedding(source_text)
        self.entries.append(
            VectorIndexEntry(
                schema=table.schema,
                table=table.name,
                vector=vector,
                source_text=source_text,
            )
        )

    def build_from_result(self, result: IntrospectionResult) -> None:
        self.entries = []
        for table in result.all_tables():
            self.add_table(table)

    def search(self, query: str, top_k: int = 5) -> list[VectorCandidate]:
        query_vector = self.generator.generate_embedding(query)
        candidates = [
            VectorCandidate(
                schema=entry.schema,
                table=entry.table,
                similarity=self.cosine_similarity(query_vector, entry.vector),
                source_text=entry.source_text,
            )
            for entry in self.entries
        ]
        candidates.sort(key=lambda item: (-item.similarity, item.qualified_name))
        return candidates[: max(0, top_k)]

    def save(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": _VECTOR_FORMAT_VERSION,
            "provider": self.generator.provider_name,
            "model": self.generator.model_name,
            "entries": [entry.to_dict() for entry in self.entries],
        }
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @classmethod
    def from_path(cls, path: str | Path, generator: EmbeddingGenerator) -> VectorSearch:
        """Load a persisted vector index from disk."""

        return cls.load(path, generator)

    @classmethod
    def load(cls, path: str | Path, generator: EmbeddingGenerator) -> VectorSearch:
        source = Path(path)
        payload = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Vector index payload must be a JSON object.")
        if int(payload.get("version", -1)) != _VECTOR_FORMAT_VERSION:
            raise ValueError("Vector index version is incompatible.")
        entries_payload = payload.get("entries")
        if not isinstance(entries_payload, list):
            raise ValueError("Vector index payload is missing the entries list.")

        index = cls(generator)
        index.entries = [
            VectorIndexEntry(
                schema=str(item["schema"]),
                table=str(item["table"]),
                vector=[float(value) for value in item["vector"]],
                source_text=str(item["source_text"]),
            )
            for item in entries_payload
            if isinstance(item, dict)
        ]
        return index

    @staticmethod
    def cosine_similarity(left: list[float], right: list[float]) -> float:
        if len(left) != len(right):
            return 0.0
        left_norm = math.sqrt(sum(value * value for value in left))
        right_norm = math.sqrt(sum(value * value for value in right))
        if left_norm == 0.0 or right_norm == 0.0:
            return 0.0
        similarity = sum(a * b for a, b in zip(left, right, strict=True)) / (left_norm * right_norm)
        return max(-1.0, min(1.0, similarity))

    @staticmethod
    def _build_source_text(table: TableInfo) -> str | None:
        semantic_material = [
            table.semantic_short,
            table.semantic_detailed,
            table.semantic_domain,
            table.semantic_role,
            table.heuristic_type,
        ]
        if not any(value and str(value).strip() for value in semantic_material) and not table.columns:
            return None

        column_names = ", ".join(column.name for column in table.columns[:12]) or "none"
        lines = [
            f"Table: {table.qualified_name}.",
            f"Short description: {table.semantic_short or 'unknown'}.",
            f"Detailed description: {table.semantic_detailed or 'unknown'}.",
            f"Semantic domain: {table.semantic_domain or 'unknown'}.",
            f"Semantic role: {table.semantic_role or 'unknown'}.",
            f"Heuristic type: {table.heuristic_type or 'unknown'}.",
            f"Columns: {column_names}.",
        ]
        return " ".join(lines)
