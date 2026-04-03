"""Semantic firewall for LLM sample preparation."""

from __future__ import annotations

import re
from typing import Any

from atlas.config import PrivacyMode
from atlas.types import ColumnInfo, TableInfo

REGEX_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
REGEX_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
REGEX_CPF = re.compile(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$")
REGEX_CPF_RAW = re.compile(r"^\d{11}$")
REGEX_CNPJ = re.compile(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")
REGEX_CNPJ_RAW = re.compile(r"^\d{14}$")
REGEX_ISO_DATE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
)
REGEX_CURRENCY_BR = re.compile(r"^R?\$\s*[\d.,]+$")

_PII_TAGS: frozenset[str] = frozenset({"EMAIL", "CPF_BR", "CNPJ_BR"})
_SAMPLE_BUDGET_CHARS = 800
_SUMMARY_EXCERPT_LIMIT = 120


class SamplePreparer:
    """Sanitize and summarize raw sample rows for safe LLM consumption."""

    def __init__(self, max_distinct_values: int = 20) -> None:
        if not 1 <= max_distinct_values <= 100:
            raise ValueError(
                f"max_distinct_values must be between 1 and 100, got {max_distinct_values}."
            )
        self.max_distinct_values = max_distinct_values

    def detect_pattern(self, value: str) -> str | None:
        """Return a canonical pattern tag for a structured value, if recognized."""
        if not value or value in {"***", "None", "null", ""}:
            return None
        normalized = value.strip()
        if REGEX_UUID.match(normalized):
            return "UUID"
        if REGEX_EMAIL.match(normalized):
            return "EMAIL"
        if REGEX_CPF.match(normalized):
            return "CPF_BR"
        if REGEX_CNPJ.match(normalized):
            return "CNPJ_BR"
        if REGEX_ISO_DATE.match(normalized):
            return "ISO_DATE"
        if REGEX_CURRENCY_BR.match(normalized):
            return "CURRENCY_BR"
        if REGEX_CPF_RAW.match(normalized) and normalized != "0" * 11:
            return "CPF_BR"
        if REGEX_CNPJ_RAW.match(normalized) and normalized != "0" * 14:
            return "CNPJ_BR"
        return None

    def _format_samples(
        self,
        col_name: str,
        sample_rows: list[dict[str, Any]],
        privacy_mode: PrivacyMode,
    ) -> tuple[str, str]:
        """Return a Python-list-like samples string and a pattern hint."""
        if privacy_mode in (PrivacyMode.stats_only, PrivacyMode.no_samples):
            return "[]", "none"

        distinct_values, detected_patterns = self._collect_distinct_values(col_name, sample_rows)

        if not distinct_values:
            return "[]", "none"

        sample_items: list[str] = []
        total_chars = 0
        remaining = 0
        for index, value in enumerate(distinct_values):
            entry = repr(value)
            total_chars += len(entry) + 2
            if total_chars > _SAMPLE_BUDGET_CHARS:
                remaining = len(distinct_values) - index
                break
            sample_items.append(entry)

        samples = "[" + ", ".join(sample_items) + "]"
        if remaining:
            samples += f"  # +{remaining} more"

        pattern_hint = " | ".join(sorted(detected_patterns)) if detected_patterns else "none"
        return samples, pattern_hint

    def _collect_distinct_values(
        self,
        col_name: str,
        sample_rows: list[dict[str, Any]],
    ) -> tuple[list[str], set[str]]:
        distinct_values: list[str] = []
        seen: set[str] = set()
        detected_patterns: set[str] = set()

        for row in sample_rows:
            raw_value = row.get(col_name)
            if raw_value is None:
                continue
            string_value = str(raw_value).strip()
            if not string_value or string_value in seen:
                continue
            seen.add(string_value)

            pattern = self.detect_pattern(string_value)
            if pattern is not None:
                detected_patterns.add(pattern)

            display_value = (
                f"[PATTERN: {pattern}]" if pattern in _PII_TAGS else string_value
            )
            distinct_values.append(display_value)
            if len(distinct_values) >= self.max_distinct_values:
                break

        return distinct_values, detected_patterns

    @staticmethod
    def _clip_excerpt(value: str) -> str:
        collapsed = " ".join(value.split())
        if len(collapsed) <= _SUMMARY_EXCERPT_LIMIT:
            return collapsed
        return collapsed[: _SUMMARY_EXCERPT_LIMIT - 3].rstrip() + "..."

    def _summarize_samples(
        self,
        col_name: str,
        sample_rows: list[dict[str, Any]],
        privacy_mode: PrivacyMode,
    ) -> str:
        if privacy_mode in (PrivacyMode.stats_only, PrivacyMode.no_samples):
            return "No live samples available."

        distinct_values, detected_patterns = self._collect_distinct_values(col_name, sample_rows)
        if not distinct_values:
            return "No non-null sample values."

        lengths = [len(value) for value in distinct_values]
        word_counts = [len(value.split()) for value in distinct_values if value.strip()]
        excerpts = ", ".join(repr(self._clip_excerpt(value)) for value in distinct_values[:3])

        parts = [f"{len(distinct_values)} distinct non-null example(s)"]
        if detected_patterns:
            parts.append(f"patterns: {' | '.join(sorted(detected_patterns))}")
        parts.append(
            f"lengths: {min(lengths)}-{max(lengths)} chars (avg {sum(lengths) / len(lengths):.0f})"
        )
        if word_counts and max(word_counts) >= 8:
            parts.append(
                f"word counts: {min(word_counts)}-{max(word_counts)} (avg {sum(word_counts) / len(word_counts):.0f})"
            )
        parts.append(f"examples: {excerpts}")
        return "; ".join(parts)

    def prepare_column_context(
        self,
        column: ColumnInfo,
        sample_rows: list[dict[str, Any]],
        privacy_mode: PrivacyMode,
    ) -> dict[str, Any]:
        """Build a sanitized context dictionary for a single column."""
        samples, pattern_hint = self._format_samples(column.name, sample_rows, privacy_mode)
        stats = column.stats
        return {
            "column_name": column.name,
            "canonical_type": column.canonical_type.value if column.canonical_type else "unknown",
            "native_type": column.native_type,
            "comment": column.comment or "none",
            "nullable": str(column.is_nullable),
            "is_unique": str(column.is_unique),
            "is_indexed": str(column.is_indexed),
            "distinct": str(stats.distinct_count if stats else 0),
            "null_rate": f"{(stats.null_rate if stats else 0.0):.1%}",
            "avg_length": f"{(stats.avg_length if stats else 0.0):.1f}",
            "pattern": pattern_hint,
            "sample_summary": self._summarize_samples(column.name, sample_rows, privacy_mode),
            "samples": samples,
        }

    def prepare_table_context(
        self,
        table: TableInfo,
        sample_rows: list[dict[str, Any]],
        privacy_mode: PrivacyMode,
    ) -> dict[str, Any]:
        """Build a sanitized context dictionary for a table prompt."""
        del sample_rows, privacy_mode

        top_columns: list[str] = []
        for column in table.columns[:10]:
            canonical = column.canonical_type.value if column.canonical_type else column.native_type
            flags: list[str] = []
            if column.is_primary_key:
                flags.append("PK")
            if column.is_foreign_key:
                flags.append("FK")
            if not column.is_nullable:
                flags.append("NOT NULL")
            suffix = f" [{', '.join(flags)}]" if flags else ""
            top_columns.append(f"{column.name} ({canonical}){suffix}")

        fk_parts: list[str] = []
        for foreign_key in table.foreign_keys:
            src_columns = ", ".join(foreign_key.source_columns)
            target = f"{foreign_key.target_schema}.{foreign_key.target_table}"
            target_columns = ", ".join(foreign_key.target_columns)
            fk_parts.append(f"({src_columns}) -> {target}({target_columns})")

        return {
            "table_name": table.name,
            "schema": table.schema,
            "table_type": table.table_type.value,
            "row_count": str(table.row_count_estimate),
            "top_columns_summary": ", ".join(top_columns) if top_columns else "none",
            "fk_summary": "; ".join(fk_parts) if fk_parts else "none declared",
            "heuristic_classification": table.heuristic_type or "unknown",
        }
