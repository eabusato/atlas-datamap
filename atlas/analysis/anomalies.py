"""Structural anomaly detection over Atlas metadata."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from enum import IntEnum

from atlas.types import IntrospectionResult, TableInfo

_RE_STAGING_NAME = re.compile(r"(?i)(?:^(?:stg|tmp|temp|stage|stge)_|_(?:stg|tmp|temp|stage)$)")
_AMBIGUOUS_NAMES = {
    "valor",
    "dado",
    "dado1",
    "dado2",
    "info",
    "campo",
    "campo1",
    "campo2",
    "campo3",
    "data",
    "data1",
    "registro",
    "value",
    "item",
    "temp",
    "obj",
    "objeto",
    "texto",
    "number",
    "numero",
    "numero1",
    "col1",
    "col2",
    "col3",
    "field1",
    "field2",
    "field3",
}
_WIDE_TABLE_THRESHOLD = 50
_HIGH_NULLABLE_THRESHOLD = 0.5
_VIEW_TYPES = {"view", "materialized_view", "foreign_table"}


class AnomalySeverity(IntEnum):
    INFO = 1
    WARNING = 2
    CRITICAL = 3

    def __str__(self) -> str:
        return self.name.lower()


@dataclass(slots=True)
class StructuralAnomaly:
    anomaly_type: str
    severity: AnomalySeverity
    schema: str
    table: str
    description: str
    suggestion: str
    column: str | None = None

    @property
    def location(self) -> str:
        if self.column is None:
            return f"{self.schema}.{self.table}"
        return f"{self.schema}.{self.table}.{self.column}"

    def to_dict(self) -> dict[str, object]:
        return {
            "anomaly_type": self.anomaly_type,
            "severity": str(self.severity),
            "schema": self.schema,
            "table": self.table,
            "column": self.column,
            "location": self.location,
            "description": self.description,
            "suggestion": self.suggestion,
        }


def _is_view(table: TableInfo) -> bool:
    return table.table_type.value in _VIEW_TYPES


def _is_staging(table: TableInfo) -> bool:
    return bool(_RE_STAGING_NAME.search(table.name.lower())) or table.heuristic_type == "staging"


def _indexed_columns(table: TableInfo) -> set[str]:
    return {column.lower() for index in table.indexes for column in index.columns}


class AnomalyDetector:
    """Detect structural issues that deserve review in atlas metadata."""

    def detect_table(self, table: TableInfo) -> list[StructuralAnomaly]:
        anomalies: list[StructuralAnomaly] = []
        is_view = _is_view(table)
        has_pk = any(column.is_primary_key for column in table.columns)
        indexed_columns = _indexed_columns(table)
        nullable_ratio = (
            sum(1 for column in table.columns if column.is_nullable) / len(table.columns)
            if table.columns
            else 0.0
        )

        if not is_view and not table.indexes:
            severity = AnomalySeverity.INFO if _is_staging(table) else AnomalySeverity.WARNING
            anomalies.append(
                StructuralAnomaly(
                    anomaly_type="no_indexes",
                    severity=severity,
                    schema=table.schema,
                    table=table.name,
                    description="Table has no indexes declared.",
                    suggestion="Add a primary key or indexes for joins and lookup predicates.",
                )
            )

        if not is_view and not has_pk:
            anomalies.append(
                StructuralAnomaly(
                    anomaly_type="no_pk",
                    severity=AnomalySeverity.WARNING,
                    schema=table.schema,
                    table=table.name,
                    description="Table does not declare a primary key.",
                    suggestion="Declare a stable primary key to support identity and joins.",
                )
            )

        if not is_view and not has_pk and nullable_ratio > _HIGH_NULLABLE_THRESHOLD:
            anomalies.append(
                StructuralAnomaly(
                    anomaly_type="high_nullable_no_pk",
                    severity=AnomalySeverity.WARNING,
                    schema=table.schema,
                    table=table.name,
                    description="More than half of the columns are nullable and the table has no primary key.",
                    suggestion="Review the model and add a primary key plus clearer nullability constraints.",
                )
            )

        for column in table.columns:
            lowered = column.name.lower()
            if lowered in _AMBIGUOUS_NAMES:
                anomalies.append(
                    StructuralAnomaly(
                        anomaly_type="ambiguous_column_name",
                        severity=AnomalySeverity.INFO,
                        schema=table.schema,
                        table=table.name,
                        column=column.name,
                        description=f"Column name {column.name!r} is too generic for reliable interpretation.",
                        suggestion="Rename the column to reflect its business meaning.",
                    )
                )

        for foreign_key in table.foreign_keys:
            required_columns = {column.lower() for column in foreign_key.source_columns}
            if not required_columns.issubset(indexed_columns):
                anomalies.append(
                    StructuralAnomaly(
                        anomaly_type="fk_without_index",
                        severity=AnomalySeverity.WARNING,
                        schema=table.schema,
                        table=table.name,
                        description=(
                            f"Foreign key {foreign_key.name!r} is not covered by indexes on "
                            f"{', '.join(foreign_key.source_columns)}."
                        ),
                        suggestion="Create an index covering the foreign key source columns.",
                    )
                )

        if not is_view and table.row_count_estimate == 0 and not _is_staging(table):
            anomalies.append(
                StructuralAnomaly(
                    anomaly_type="empty_table",
                    severity=AnomalySeverity.INFO,
                    schema=table.schema,
                    table=table.name,
                    description="Table is structurally present but currently empty.",
                    suggestion="Confirm whether the table is still in active use or should be archived.",
                )
            )

        declared_fk_columns = {
            source_column.lower()
            for foreign_key in table.foreign_keys
            for source_column in foreign_key.source_columns
        }
        for column in table.columns:
            lowered = column.name.lower()
            if not lowered.endswith("_id"):
                continue
            if lowered in declared_fk_columns:
                continue
            if column.is_primary_key:
                continue
            anomalies.append(
                StructuralAnomaly(
                    anomaly_type="implicit_fk",
                    severity=AnomalySeverity.INFO,
                    schema=table.schema,
                    table=table.name,
                    column=column.name,
                    description="Column looks like a foreign key but no declared relationship was found.",
                    suggestion="Declare a foreign key or rename the column if it is not relational.",
                )
            )

        if len(table.columns) > _WIDE_TABLE_THRESHOLD:
            anomalies.append(
                StructuralAnomaly(
                    anomaly_type="wide_table",
                    severity=AnomalySeverity.INFO,
                    schema=table.schema,
                    table=table.name,
                    description=f"Table has {len(table.columns)} columns, above the wide-table threshold.",
                    suggestion="Consider vertical decomposition or grouping columns by clearer responsibilities.",
                )
            )

        return anomalies

    def detect(self, result: IntrospectionResult) -> list[StructuralAnomaly]:
        anomalies: list[StructuralAnomaly] = []
        for table in result.all_tables():
            anomalies.extend(self.detect_table(table))
        anomalies.sort(
            key=lambda item: (-int(item.severity), item.schema, item.table, item.anomaly_type)
        )
        return anomalies

    def summarize(self, anomalies: list[StructuralAnomaly]) -> dict[str, int]:
        counts = Counter(anomaly.anomaly_type for anomaly in anomalies)
        return dict(sorted(counts.items()))
