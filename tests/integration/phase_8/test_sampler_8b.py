"""Tests for the semantic firewall / sample preparer (Phase 8B)."""

from __future__ import annotations

from typing import Any

import pytest

from atlas.ai import SamplePreparer
from atlas.ai.sampler import _PII_TAGS
from atlas.config import PrivacyMode
from atlas.types import (
    AtlasType,
    ColumnInfo,
    ColumnStats,
    ForeignKeyInfo,
    TableInfo,
    TableType,
)


@pytest.fixture
def preparer() -> SamplePreparer:
    return SamplePreparer(max_distinct_values=5)


@pytest.fixture
def email_column() -> ColumnInfo:
    return ColumnInfo(
        name="email",
        native_type="varchar",
        canonical_type=AtlasType.TEXT,
        is_nullable=True,
        stats=ColumnStats(row_count=100, null_count=5, distinct_count=80),
    )


@pytest.fixture
def id_column() -> ColumnInfo:
    return ColumnInfo(
        name="id",
        native_type="integer",
        canonical_type=AtlasType.INTEGER,
        is_nullable=False,
        is_primary_key=True,
        stats=ColumnStats(row_count=100, null_count=0, distinct_count=100),
    )


@pytest.fixture
def simple_table() -> TableInfo:
    return TableInfo(
        name="users",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=5000,
        columns=[
            ColumnInfo(
                name="id",
                native_type="integer",
                canonical_type=AtlasType.INTEGER,
                is_primary_key=True,
                is_nullable=False,
            ),
            ColumnInfo(
                name="email",
                native_type="varchar",
                canonical_type=AtlasType.TEXT,
            ),
            ColumnInfo(
                name="created_at",
                native_type="timestamp",
                canonical_type=AtlasType.TIMESTAMP,
            ),
        ],
        heuristic_type="dimension",
    )


@pytest.fixture
def sample_rows() -> list[dict[str, Any]]:
    return [
        {"id": 1, "email": "alice@example.com", "created_at": "2024-01-01T00:00:00Z"},
        {"id": 2, "email": "bob@example.com", "created_at": "2024-01-02T00:00:00Z"},
        {"id": 3, "email": "carol@example.com", "created_at": "2024-01-03T00:00:00Z"},
        {"id": 4, "email": "dave@example.com", "created_at": "2024-01-04T00:00:00Z"},
        {"id": 5, "email": "eve@example.com", "created_at": "2024-01-05T00:00:00Z"},
        {"id": 6, "email": "frank@example.com", "created_at": "2024-01-06T00:00:00Z"},
    ]


@pytest.mark.integration
@pytest.mark.phase_8b
class TestSamplePreparerConstruction:
    def test_invalid_distinct_limit_raises(self) -> None:
        with pytest.raises(ValueError, match="max_distinct_values"):
            SamplePreparer(max_distinct_values=0)


@pytest.mark.integration
@pytest.mark.phase_8b
class TestDetectPattern:
    def test_detects_structured_patterns(self, preparer: SamplePreparer) -> None:
        assert preparer.detect_pattern("alice@example.com") == "EMAIL"
        assert preparer.detect_pattern("f47ac10b-58cc-4372-a567-0e02b2c3d479") == "UUID"
        assert preparer.detect_pattern("123.456.789-09") == "CPF_BR"
        assert preparer.detect_pattern("12.345.678/0001-95") == "CNPJ_BR"
        assert preparer.detect_pattern("2024-01-15T10:30:00Z") == "ISO_DATE"
        assert preparer.detect_pattern("R$ 1.250,00") == "CURRENCY_BR"

    def test_pii_tag_set_covers_only_sensitive_values(self) -> None:
        assert {"EMAIL", "CPF_BR", "CNPJ_BR"} <= _PII_TAGS
        assert "UUID" not in _PII_TAGS


@pytest.mark.integration
@pytest.mark.phase_8b
class TestPrepareColumnContext:
    def test_stats_only_suppresses_samples(
        self,
        preparer: SamplePreparer,
        email_column: ColumnInfo,
        sample_rows: list[dict[str, Any]],
    ) -> None:
        context = preparer.prepare_column_context(email_column, sample_rows, PrivacyMode.stats_only)
        assert context["samples"] == "[]"
        assert context["pattern"] == "none"

    def test_no_samples_suppresses_samples(
        self,
        preparer: SamplePreparer,
        email_column: ColumnInfo,
        sample_rows: list[dict[str, Any]],
    ) -> None:
        context = preparer.prepare_column_context(email_column, sample_rows, PrivacyMode.no_samples)
        assert context["samples"] == "[]"

    def test_normal_mode_masks_email_values_with_pattern_tags(
        self,
        preparer: SamplePreparer,
        email_column: ColumnInfo,
        sample_rows: list[dict[str, Any]],
    ) -> None:
        context = preparer.prepare_column_context(email_column, sample_rows, PrivacyMode.normal)
        assert "alice@example.com" not in context["samples"]
        assert "[PATTERN: EMAIL]" in context["samples"]
        assert context["pattern"] == "EMAIL"

    def test_uuid_values_are_kept_but_tagged(
        self,
        preparer: SamplePreparer,
        id_column: ColumnInfo,
    ) -> None:
        rows = [{"id": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}]
        context = preparer.prepare_column_context(id_column, rows, PrivacyMode.normal)
        assert "UUID" in context["pattern"]
        assert "f47ac10b-58cc-4372-a567-0e02b2c3d479" in context["samples"]

    def test_statistics_are_formatted(
        self,
        preparer: SamplePreparer,
        email_column: ColumnInfo,
        sample_rows: list[dict[str, Any]],
    ) -> None:
        context = preparer.prepare_column_context(email_column, sample_rows, PrivacyMode.normal)
        assert context["distinct"] == "80"
        assert context["null_rate"] == "5.0%"
        assert context["nullable"] == "True"
        assert context["canonical_type"] == "text"
        assert context["comment"] == "none"
        assert context["is_indexed"] == "False"
        assert context["is_unique"] == "False"

    def test_sample_summary_reports_text_shape(
        self,
        preparer: SamplePreparer,
    ) -> None:
        column = ColumnInfo(name="corpo", native_type="text", canonical_type=AtlasType.CLOB)
        rows = [
            {
                "corpo": (
                    "Era uma vez uma cidade submersa onde cada morador guardava uma historia "
                    "inteira dentro de uma garrafa azul esquecida no cais."
                )
            },
            {
                "corpo": (
                    "No fim da tarde, o protagonista voltou para casa carregando cartas, "
                    "lembrancas e o peso silencioso de um segredo antigo."
                )
            },
        ]
        context = preparer.prepare_column_context(column, rows, PrivacyMode.normal)
        assert "2 distinct non-null example(s)" in context["sample_summary"]
        assert "lengths:" in context["sample_summary"]
        assert "examples:" in context["sample_summary"]

    def test_duplicate_values_are_deduplicated(
        self,
        preparer: SamplePreparer,
        id_column: ColumnInfo,
    ) -> None:
        context = preparer.prepare_column_context(
            id_column,
            [{"id": "42"} for _ in range(10)],
            PrivacyMode.normal,
        )
        assert context["samples"].count("42") == 1

    def test_max_distinct_limit_is_respected(
        self,
        preparer: SamplePreparer,
        id_column: ColumnInfo,
    ) -> None:
        context = preparer.prepare_column_context(
            id_column,
            [{"id": str(index)} for index in range(10)],
            PrivacyMode.normal,
        )
        assert context["samples"].count("'") <= preparer.max_distinct_values * 2 + 2


@pytest.mark.integration
@pytest.mark.phase_8b
class TestPrepareTableContext:
    def test_table_context_includes_required_fields(
        self,
        preparer: SamplePreparer,
        simple_table: TableInfo,
    ) -> None:
        context = preparer.prepare_table_context(simple_table, [], PrivacyMode.normal)
        assert context["table_name"] == "users"
        assert context["schema"] == "public"
        assert context["table_type"] == "table"
        assert context["row_count"] == "5000"
        assert context["heuristic_classification"] == "dimension"

    def test_table_context_includes_column_flags(
        self,
        preparer: SamplePreparer,
        simple_table: TableInfo,
    ) -> None:
        context = preparer.prepare_table_context(simple_table, [], PrivacyMode.normal)
        assert "id (integer) [PK, NOT NULL]" in context["top_columns_summary"]

    def test_table_context_includes_foreign_keys(self, preparer: SamplePreparer) -> None:
        table = TableInfo(
            name="orders",
            schema="public",
            foreign_keys=[
                ForeignKeyInfo(
                    name="fk_orders_user",
                    source_schema="public",
                    source_table="orders",
                    source_columns=["user_id"],
                    target_schema="public",
                    target_table="users",
                    target_columns=["id"],
                )
            ],
        )
        context = preparer.prepare_table_context(table, [], PrivacyMode.normal)
        assert context["fk_summary"] == "(user_id) -> public.users(id)"

    def test_table_context_falls_back_when_heuristic_type_missing(
        self,
        preparer: SamplePreparer,
    ) -> None:
        context = preparer.prepare_table_context(
            TableInfo(name="unknown_tbl", schema="public"),
            [],
            PrivacyMode.normal,
        )
        assert context["heuristic_classification"] == "unknown"

    def test_table_context_limits_column_summary_to_ten(self, preparer: SamplePreparer) -> None:
        table = TableInfo(
            name="wide_tbl",
            schema="public",
            columns=[
                ColumnInfo(
                    name=f"col{index}",
                    native_type="integer",
                    canonical_type=AtlasType.INTEGER,
                )
                for index in range(20)
            ],
        )
        context = preparer.prepare_table_context(table, [], PrivacyMode.normal)
        assert "col9" in context["top_columns_summary"]
        assert "col10" not in context["top_columns_summary"]
