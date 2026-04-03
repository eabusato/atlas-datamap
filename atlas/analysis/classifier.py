"""Heuristic table classifier used by Atlas analysis flows."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field

from atlas.types import AtlasType, IntrospectionResult, TableInfo

PROBABLE_TYPES = (
    "staging",
    "config",
    "pivot",
    "log",
    "fact",
    "domain_main",
    "dimension",
    "unknown",
)
_TYPE_PRIORITY = {name: index for index, name in enumerate(PROBABLE_TYPES)}
_TIE_BREAK_PRIORITY = {
    "staging": 0,
    "log": 1,
    "fact": 2,
    "pivot": 3,
    "dimension": 4,
    "config": 5,
    "domain_main": 6,
    "unknown": 7,
}

_RE_STAGING = re.compile(r"(?i)(?:^(?:stg|tmp|temp|stage|stge)_|_(?:stg|tmp|temp|stage)$)")
_RE_LOG = re.compile(
    r"(?i)(?:log|audit|hist(?:ory)?|trail|registro|events?|changelog)"
)
_RE_CONFIG = re.compile(
    r"(?i)(?:config|param|setting|option|lookup|reference|(?:^|_)ref(?:_|$)|catalog)"
)
_RE_FACT = re.compile(r"(?i)(?:^(?:fact|fato|fct|ft)_|_(?:fact|fatos)$)")
_RE_DIMENSION = re.compile(r"(?i)(?:^(?:dim|dimension|dimensao)_|_(?:dim|dimension)$)")
_RE_PIVOT = re.compile(r"(?i)(?:_rel_|_has_|_map$|_x_)")

_EVENT_COL_PATTERNS = (
    "action",
    "event",
    "operation",
    "operacao",
    "tipo_evento",
    "event_type",
    "status_change",
    "change_type",
)
_FACT_NUMERIC_PATTERNS = (
    "amount",
    "qty",
    "quantity",
    "price",
    "revenue",
    "cost",
    "total",
    "subtotal",
    "gross",
    "net",
    "discount",
    "tax",
    "valor",
    "preco",
    "custo",
    "receita",
    "faturamento",
    "quantidade",
)
_CONFIG_KEY_PATTERNS = ("key", "name", "code", "parameter")
_CONFIG_VAL_PATTERNS = ("value", "description", "label", "content")
_DATETIME_TYPES = {
    AtlasType.DATETIME.value,
    AtlasType.TIMESTAMP.value,
    AtlasType.DATE.value,
    AtlasType.TIME.value,
    AtlasType.INTERVAL.value,
}
_NUMERIC_TYPES = {
    AtlasType.INTEGER.value,
    AtlasType.SMALLINT.value,
    AtlasType.BIGINT.value,
    AtlasType.TINYINT.value,
    AtlasType.FLOAT.value,
    AtlasType.DOUBLE.value,
    AtlasType.DECIMAL.value,
    AtlasType.MONEY.value,
}
_TEXT_TYPES = {
    AtlasType.TEXT.value,
    AtlasType.CHAR.value,
    AtlasType.CLOB.value,
    AtlasType.UNKNOWN.value,
}
_CONFIG_NAME_TOKENS = ("config", "param", "setting", "option", "lookup", "reference", "catalog")
_STAGING_COLUMN_PATTERNS = ("load_date", "batch_id", "etl_", "_raw", "_landing")
_TEMPORAL_NAME_PATTERNS = (
    "created_at",
    "updated_at",
    "deleted_at",
    "occurred_at",
    "processed_at",
)


@dataclass(slots=True)
class TableClassification:
    """Structured classifier output for a single table."""

    table: str
    schema: str
    probable_type: str
    confidence: float
    signals: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "table": self.table,
            "schema": self.schema,
            "probable_type": self.probable_type,
            "confidence": round(self.confidence, 4),
            "signals": list(self.signals),
        }


@dataclass(slots=True)
class _Signal:
    probable_type: str
    weight: float
    reason: str


def _normalized_name(table: TableInfo) -> str:
    return table.name.lower()


def _col_names(table: TableInfo) -> list[str]:
    return [column.name.lower() for column in table.columns]


def _col_canonical_types(table: TableInfo) -> list[str]:
    return [
        (column.canonical_type or AtlasType.UNKNOWN).value
        for column in table.columns
    ]


def _fk_out_count(table: TableInfo) -> int:
    return len(table.foreign_keys)


def _has_pk(table: TableInfo) -> bool:
    return any(column.is_primary_key for column in table.columns)


def _pk_count(table: TableInfo) -> int:
    return sum(1 for column in table.columns if column.is_primary_key)


def _fk_source_columns(table: TableInfo) -> set[str]:
    return {
        source_column.lower()
        for foreign_key in table.foreign_keys
        for source_column in foreign_key.source_columns
    }


def _non_key_non_fk_column_count(table: TableInfo) -> int:
    fk_columns = _fk_source_columns(table)
    return sum(
        1
        for column in table.columns
        if not column.is_primary_key and column.name.lower() not in fk_columns
    )


def _datetime_ratio(table: TableInfo) -> float:
    if not table.columns:
        return 0.0
    matches = sum(1 for type_name in _col_canonical_types(table) if type_name in _DATETIME_TYPES)
    return matches / len(table.columns)


def _text_ratio(table: TableInfo) -> float:
    non_key_columns = [column for column in table.columns if not column.is_primary_key]
    if not non_key_columns:
        return 0.0
    matches = sum(
        1
        for column in non_key_columns
        if (column.canonical_type or AtlasType.UNKNOWN).value in _TEXT_TYPES
        and not _is_temporalish_column(column.name)
    )
    return matches / len(non_key_columns)


def _has_measure_like_numeric(table: TableInfo) -> bool:
    for column in table.columns:
        canonical_type = (column.canonical_type or AtlasType.UNKNOWN).value
        if canonical_type not in _NUMERIC_TYPES:
            continue
        lowered = column.name.lower()
        if any(pattern in lowered for pattern in _FACT_NUMERIC_PATTERNS):
            return True
    return False


def _has_timestamp_column(table: TableInfo) -> bool:
    return any(type_name in _DATETIME_TYPES for type_name in _col_canonical_types(table)) or any(
        _is_temporalish_column(column.name) for column in table.columns
    )


def _has_event_column(table: TableInfo) -> bool:
    names = _col_names(table)
    return any(
        any(pattern in column_name for pattern in _EVENT_COL_PATTERNS)
        for column_name in names
    )


def _has_staging_columns(table: TableInfo) -> bool:
    names = _col_names(table)
    return any(
        column_name == "load_date"
        or column_name == "batch_id"
        or column_name.startswith("etl_")
        or column_name.endswith("_raw")
        or column_name.endswith("_landing")
        for column_name in names
    )


def _is_temporalish_column(column_name: str) -> bool:
    lowered = column_name.lower()
    if lowered in _TEMPORAL_NAME_PATTERNS:
        return True
    return lowered.endswith(("_at", "_date", "_time", "_on"))


def _has_config_key_value_pattern(table: TableInfo) -> bool:
    names = set(_col_names(table))
    has_key = any(any(pattern in name for pattern in _CONFIG_KEY_PATTERNS) for name in names)
    has_value = any(any(pattern in name for pattern in _CONFIG_VAL_PATTERNS) for name in names)
    return has_key and has_value


def _pivot_name_hint(table: TableInfo) -> bool:
    lowered = _normalized_name(table)
    if _RE_PIVOT.search(lowered):
        return True
    return lowered.endswith(("_users", "_roles", "_tags"))


def _is_surrogate_pk_plus_two_fks(table: TableInfo) -> bool:
    fk_columns = _fk_source_columns(table)
    pk_columns = [column.name.lower() for column in table.columns if column.is_primary_key]
    if len(pk_columns) != 1:
        return False
    if pk_columns[0] in fk_columns:
        return False
    return len(fk_columns) >= 2


def _is_classic_dimension(table: TableInfo) -> bool:
    if not _has_pk(table):
        return False
    if _has_measure_like_numeric(table):
        return False
    non_key_columns = [column for column in table.columns if not column.is_primary_key]
    if len(non_key_columns) < 2:
        return False
    text_columns = sum(
        1
        for column in non_key_columns
        if (column.canonical_type or AtlasType.UNKNOWN).value in _TEXT_TYPES
        and not _is_temporalish_column(column.name)
    )
    return text_columns >= max(1, len(non_key_columns) // 2)


class TableClassifier:
    """Rule-based table classifier with confidence and signal reporting."""

    _TOTAL_SIGNAL_WEIGHT = {
        "staging": 1.0,
        "config": 1.0,
        "pivot": 1.0,
        "log": 1.0,
        "fact": 1.0,
        "domain_main": 1.0,
        "dimension": 1.0,
    }

    def _signals_for(self, table: TableInfo, fk_in_degree: int) -> list[_Signal]:
        signals: list[_Signal] = []
        lowered_name = _normalized_name(table)
        row_count = table.row_count_estimate
        fk_out = _fk_out_count(table)
        pk_count = _pk_count(table)
        datetime_ratio = _datetime_ratio(table)
        fk_columns = _fk_source_columns(table)
        non_key_non_fk_count = _non_key_non_fk_column_count(table)

        if _RE_STAGING.search(lowered_name):
            signals.append(_Signal("staging", 0.45, "staging-like table name"))
        if row_count < 100:
            signals.append(_Signal("staging", 0.15, "very small row count"))
        if _has_staging_columns(table):
            signals.append(_Signal("staging", 0.40, "etl or landing columns detected"))

        if _RE_CONFIG.search(lowered_name):
            signals.append(_Signal("config", 0.35, "configuration-like table name"))
        if row_count < 1000:
            signals.append(_Signal("config", 0.15, "small lookup-style row count"))
        if _has_config_key_value_pattern(table):
            signals.append(_Signal("config", 0.50, "key/value column pattern detected"))

        if fk_out in {2, 3}:
            signals.append(_Signal("pivot", 0.35, "two or three outgoing foreign keys"))
        if pk_count >= 2 and pk_count == len(fk_columns):
            signals.append(_Signal("pivot", 0.35, "composite primary key built from foreign keys"))
        elif _is_surrogate_pk_plus_two_fks(table):
            signals.append(_Signal("pivot", 0.35, "surrogate primary key plus two foreign keys"))
        if non_key_non_fk_count <= 3:
            signals.append(_Signal("pivot", 0.15, "very few non-key descriptive columns"))
        if _pivot_name_hint(table):
            signals.append(_Signal("pivot", 0.15, "relationship-oriented table name"))

        if datetime_ratio >= 0.25:
            signals.append(_Signal("log", 0.35, "high datetime column density"))
        if _has_event_column(table):
            signals.append(_Signal("log", 0.25, "event-oriented column names"))
        if _RE_LOG.search(lowered_name):
            signals.append(_Signal("log", 0.25, "log or audit table name"))
        if "updated_at" not in _col_names(table):
            signals.append(_Signal("log", 0.15, "no updated_at maintenance column"))

        if fk_out >= 3:
            signals.append(_Signal("fact", 0.30, "multiple dimensional relationships"))
        if _has_measure_like_numeric(table):
            signals.append(_Signal("fact", 0.30, "measure-like numeric columns"))
        if _has_timestamp_column(table):
            signals.append(_Signal("fact", 0.20, "time grain column detected"))
        if _RE_FACT.search(lowered_name):
            signals.append(_Signal("fact", 0.20, "fact-style table name"))

        if fk_in_degree >= 4:
            signals.append(_Signal("domain_main", 0.45, "high inbound relationship degree"))
        if row_count >= 500:
            signals.append(_Signal("domain_main", 0.20, "large operational row count"))
        if _has_pk(table):
            signals.append(_Signal("domain_main", 0.20, "stable primary key present"))
        if not _RE_STAGING.search(lowered_name) and not _RE_LOG.search(lowered_name):
            signals.append(_Signal("domain_main", 0.15, "not shaped like staging or log data"))

        if _has_pk(table):
            signals.append(_Signal("dimension", 0.20, "primary key present"))
        if fk_out <= 2:
            signals.append(_Signal("dimension", 0.15, "low outbound relationship count"))
        if not _has_measure_like_numeric(table) and _text_ratio(table) >= 0.5:
            signals.append(_Signal("dimension", 0.30, "mostly descriptive text attributes"))
        if _RE_DIMENSION.search(lowered_name):
            signals.append(_Signal("dimension", 0.20, "dimension-style table name"))
        if _is_classic_dimension(table):
            signals.append(_Signal("dimension", 0.15, "entity table with descriptive attributes"))

        return signals

    def classify(self, table: TableInfo, fk_in_degree: int) -> TableClassification:
        signals = self._signals_for(table, fk_in_degree)
        active_by_type: dict[str, list[_Signal]] = defaultdict(list)
        weight_by_type: dict[str, float] = defaultdict(float)
        for signal in signals:
            active_by_type[signal.probable_type].append(signal)
            weight_by_type[signal.probable_type] += signal.weight

        best_type = "unknown"
        best_confidence = 0.0
        for probable_type, total_weight in weight_by_type.items():
            confidence = min(
                1.0,
                total_weight / self._TOTAL_SIGNAL_WEIGHT.get(probable_type, 1.0),
            )
            if confidence > best_confidence:
                best_type = probable_type
                best_confidence = confidence
                continue
            if (
                confidence == best_confidence
                and confidence > 0.0
                and _TIE_BREAK_PRIORITY[probable_type] < _TIE_BREAK_PRIORITY[best_type]
            ):
                best_type = probable_type

        if best_confidence < 0.3:
            return TableClassification(
                table=table.name,
                schema=table.schema,
                probable_type="unknown",
                confidence=0.0,
                signals=[],
            )

        ordered_signals = sorted(
            active_by_type[best_type],
            key=lambda item: (-item.weight, item.reason),
        )
        return TableClassification(
            table=table.name,
            schema=table.schema,
            probable_type=best_type,
            confidence=best_confidence,
            signals=[signal.reason for signal in ordered_signals],
        )

    def classify_all(self, result: IntrospectionResult) -> list[TableClassification]:
        classifications: list[TableClassification] = []
        for table in result.all_tables():
            fk_in_degree = len(result.fk_in_degree_map.get(table.qualified_name, []))
            classification = self.classify(table, fk_in_degree=fk_in_degree)
            table.heuristic_type = classification.probable_type
            table.heuristic_confidence = classification.confidence
            classifications.append(classification)
        classifications.sort(
            key=lambda item: (
                item.schema,
                item.table,
                _TYPE_PRIORITY.get(item.probable_type, len(PROBABLE_TYPES)),
            )
        )
        return classifications
