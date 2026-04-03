"""Integration tests for Phase 9A semantic cache and schema enrichment."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from atlas.ai import (
    AIConfig,
    AIConnectionError,
    LocalLLMClient,
    ModelInfo,
    SemanticCache,
    SemanticEnricher,
)
from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors.base import BaseConnector
from atlas.types import ColumnInfo, SchemaInfo, TableInfo


class RoutingClient(LocalLLMClient):
    """Prompt-aware fake LLM client for deterministic schema enrichment tests."""

    def __init__(self, routes: dict[str, object]) -> None:
        super().__init__(AIConfig(provider="ollama", model="llama3"))
        self.routes = dict(routes)
        self.calls: list[str] = []

    def is_available(self) -> bool:
        return True

    def get_model_info(self) -> ModelInfo:
        return ModelInfo("ollama", "llama3", True, "1.0")

    def generate(
        self,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        del max_tokens, temperature
        self.calls.append(prompt)
        preferred_prefixes = ("Column:", "Table:") if "Column:" in prompt else ("Table:", "Column:")
        ordered_routes: list[tuple[str, object]] = []
        for prefix in preferred_prefixes:
            ordered_routes.extend(
                sorted(
                    (
                        (marker, response)
                        for marker, response in self.routes.items()
                        if marker.startswith(prefix)
                    ),
                    key=lambda item: len(item[0]),
                    reverse=True,
                )
            )
        for marker, response in ordered_routes:
            if marker in prompt:
                if isinstance(response, Exception):
                    raise response
                return str(response)
        raise AssertionError(f"Unexpected prompt: {prompt}")


class MemoryConnector(BaseConnector):
    """Minimal connector used to test enrichment orchestration."""

    def __init__(
        self,
        *,
        sample_rows: dict[tuple[str, str, tuple[str, ...] | None], list[dict[str, Any]]] | None = None,
        fail_keys: set[tuple[str, str, tuple[str, ...] | None]] | None = None,
    ) -> None:
        config = AtlasConnectionConfig(
            engine=DatabaseEngine.sqlite,
            host="",
            database=":memory:",
            privacy_mode=PrivacyMode.normal,
        )
        super().__init__(config)
        self.sample_rows = sample_rows or {}
        self.fail_keys = fail_keys or set()
        self.sample_calls: list[tuple[str, str, tuple[str, ...] | None]] = []

    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def get_schemas(self) -> list[SchemaInfo]:
        return []

    def get_tables(self, schema: str) -> list[TableInfo]:
        del schema
        return []

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        del schema, table
        return 0

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        del schema, table
        return 0

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        del schema, table
        return []

    def get_foreign_keys(self, schema: str, table: str) -> list[Any]:
        del schema, table
        return []

    def get_indexes(self, schema: str, table: str) -> list[Any]:
        del schema, table
        return []

    def get_sample_rows(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        privacy_mode: PrivacyMode | None = None,
    ) -> list[dict[str, Any]]:
        del limit, privacy_mode
        key = (schema, table, tuple(columns) if columns else None)
        self.sample_calls.append(key)
        if key in self.fail_keys:
            raise RuntimeError(f"Sampling failed for {schema}.{table}")
        return list(self.sample_rows.get(key, []))

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        del schema, table, column
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        del schema, table, column
        return 0


def _table(name: str, *columns: ColumnInfo) -> TableInfo:
    table = TableInfo(name=name, schema="sales", columns=list(columns))
    table.column_count = len(table.columns)
    return table


def _column(name: str, native_type: str = "text", *, nullable: bool = True) -> ColumnInfo:
    return ColumnInfo(name=name, native_type=native_type, is_nullable=nullable)


@pytest.fixture
def cache_dir(phase_tmp_dir: Path) -> Path:
    return phase_tmp_dir


@pytest.mark.integration
@pytest.mark.phase_9a
def test_table_cache_hit_persists_and_skips_llm(cache_dir: Path) -> None:
    table = _table("orders", _column("id", "integer", nullable=False))
    first_client = RoutingClient(
        {
            "Table: sales.orders": (
                '{"short_description":"Orders","detailed_description":"Order headers",'
                '"probable_domain":"sales","probable_role":"transaction_header","confidence":0.9}'
            )
        }
    )
    cache = SemanticCache(cache_dir)
    enricher = SemanticEnricher(first_client, cache=cache)

    enricher.enrich_table(table, [{"id": 1}], PrivacyMode.normal)
    cache.save()
    assert table.semantic_short == "Orders"
    assert len(first_client.calls) == 1

    cached = SemanticCache(cache_dir)
    second_client = RoutingClient({})
    second_table = _table("orders", _column("id", "integer", nullable=False))
    SemanticEnricher(second_client, cache=cached).enrich_table(
        second_table,
        [{"id": 1}],
        PrivacyMode.normal,
    )

    assert second_table.semantic_short == "Orders"
    assert second_client.calls == []


@pytest.mark.integration
@pytest.mark.phase_9a
def test_table_signature_invalidation_on_structural_change(cache_dir: Path) -> None:
    cache = SemanticCache(cache_dir)
    original = _table("orders", _column("id", "integer", nullable=False))
    cache.put_table_payload(
        original,
        {"short_description": "Orders", "detailed_description": "headers", "confidence": 1.0},
    )
    cache.save()

    changed = _table(
        "orders",
        _column("id", "bigint", nullable=False),
        _column("status", "text"),
    )
    reloaded = SemanticCache(cache_dir)
    assert reloaded.get_table_payload(changed) is None


@pytest.mark.integration
@pytest.mark.phase_9a
def test_column_cache_hit_skips_llm(cache_dir: Path) -> None:
    table = _table("orders", _column("status"))
    column = table.columns[0]
    first_client = RoutingClient(
        {
            "Column: status": (
                '{"short_description":"Order status","detailed_description":"Lifecycle state",'
                '"probable_role":"status","confidence":0.88}'
            )
        }
    )
    cache = SemanticCache(cache_dir)
    enricher = SemanticEnricher(first_client, cache=cache)

    enricher.enrich_column(table, column, [{"status": "paid"}], PrivacyMode.normal)
    cache.save()
    assert len(first_client.calls) == 1

    second_client = RoutingClient({})
    reloaded = SemanticCache(cache_dir)
    second_table = _table("orders", _column("status"))
    SemanticEnricher(second_client, cache=reloaded).enrich_column(
        second_table,
        second_table.columns[0],
        [{"status": "paid"}],
        PrivacyMode.normal,
    )
    assert second_table.columns[0].semantic_short == "Order status"
    assert second_client.calls == []


@pytest.mark.integration
@pytest.mark.phase_9a
def test_enrich_column_repairs_generic_long_form_text_answers(cache_dir: Path) -> None:
    table = _table(
        "contos_conto",
        _column("titulo"),
        _column("corpo"),
        _column("autor_id", "integer"),
    )
    table.semantic_short = "Stories"
    table.semantic_detailed = "Stories written by platform authors."
    table.semantic_role = "content_catalog"
    corpo = next(column for column in table.columns if column.name == "corpo")
    client = RoutingClient(
        {
            "Column: corpo": (
                '{"short_description":"Text content of the body of a document or message.",'
                '"detailed_description":"The `corpo` column stores the actual text content of messages, emails, or documents within the `sales.contos_conto` table.",'
                '"probable_role":"Content Storage","confidence":1.0}'
            )
        }
    )
    sample_rows = [
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

    SemanticEnricher(client, cache=SemanticCache(cache_dir)).enrich_column(
        table,
        corpo,
        sample_rows,
        PrivacyMode.normal,
        force_recompute=True,
    )

    assert corpo.semantic_short == "Story body text"
    assert corpo.semantic_role == "narrative_content"
    assert (
        corpo.semantic_detailed
        == "Stores the full narrative text/body of each story in sales.contos_conto."
    )


@pytest.mark.integration
@pytest.mark.phase_9a
def test_enrich_schema_parallel_workers_updates_tables_and_columns(cache_dir: Path) -> None:
    schema = SchemaInfo(
        name="sales",
        engine="sqlite",
        tables=[
            _table("orders", _column("status"), _column("total", "numeric")),
            _table("customers", _column("email"), _column("name")),
        ],
    )
    connector = MemoryConnector(
        sample_rows={
            ("sales", "orders", None): [{"status": "paid", "total": "12.30"}],
            ("sales", "orders", ("status",)): [{"status": "paid"}],
            ("sales", "orders", ("total",)): [{"total": "12.30"}],
            ("sales", "customers", None): [{"email": "alice@example.com", "name": "Alice"}],
            ("sales", "customers", ("email",)): [{"email": "alice@example.com"}],
            ("sales", "customers", ("name",)): [{"name": "Alice"}],
        }
    )
    client = RoutingClient(
        {
            "Table: sales.orders": (
                '{"short_description":"Orders","detailed_description":"Order headers",'
                '"probable_domain":"sales","probable_role":"transaction_header","confidence":0.93}'
            ),
            "Table: sales.customers": (
                '{"short_description":"Customers","detailed_description":"Customer master data",'
                '"probable_domain":"sales","probable_role":"dimension","confidence":0.91}'
            ),
            "Column: status": (
                '{"short_description":"Order status","detailed_description":"Current state",'
                '"probable_role":"status","confidence":0.82}'
            ),
            "Column: total": (
                '{"short_description":"Order total","detailed_description":"Monetary total",'
                '"probable_role":"amount","confidence":0.84}'
            ),
            "Column: email": (
                '{"short_description":"Customer email","detailed_description":"Primary email",'
                '"probable_role":"email","confidence":0.95}'
            ),
            "Column: name": (
                '{"short_description":"Customer name","detailed_description":"Display name",'
                '"probable_role":"name","confidence":0.9}'
            ),
        }
    )
    progress: list[tuple[str, int, int]] = []

    enricher = SemanticEnricher(client, cache=SemanticCache(cache_dir))
    enriched = enricher.enrich_schema(
        schema,
        connector,
        PrivacyMode.normal,
        parallel_workers=2,
        on_table_complete=lambda table, current, total: progress.append(
            (table.name, current, total)
        ),
    )

    assert enriched.tables[0].semantic_short == "Orders"
    assert enriched.tables[1].semantic_short == "Customers"
    assert enriched.tables[0].columns[0].semantic_role == "status"
    assert enriched.tables[0].columns[1].semantic_role == "amount"
    assert enriched.tables[1].columns[0].semantic_role == "email"
    assert len(progress) == 2
    assert all(total == 2 for _, _, total in progress)
    assert (cache_dir / ".semantic_cache.json").exists()


@pytest.mark.integration
@pytest.mark.phase_9a
def test_sampling_failures_degrade_to_empty_samples_without_aborting(cache_dir: Path) -> None:
    schema = SchemaInfo(
        name="sales",
        engine="sqlite",
        tables=[_table("orders", _column("status"))],
    )
    connector = MemoryConnector(fail_keys={("sales", "orders", None), ("sales", "orders", ("status",))})
    client = RoutingClient(
        {
            "Table: sales.orders": (
                '{"short_description":"Orders","detailed_description":"Still works",'
                '"probable_domain":"sales","probable_role":"transaction_header","confidence":0.8}'
            ),
            "Column: status": (
                '{"short_description":"Status","detailed_description":"Still works",'
                '"probable_role":"status","confidence":0.8}'
            ),
        }
    )

    SemanticEnricher(client, cache=SemanticCache(cache_dir)).enrich_schema(
        schema,
        connector,
        PrivacyMode.normal,
        parallel_workers=1,
    )

    assert schema.tables[0].semantic_short == "Orders"
    assert schema.tables[0].columns[0].semantic_short == "Status"


@pytest.mark.integration
@pytest.mark.phase_9a
def test_ai_error_does_not_abort_other_tables(cache_dir: Path) -> None:
    schema = SchemaInfo(
        name="sales",
        engine="sqlite",
        tables=[
            _table("orders", _column("status")),
            _table("customers", _column("name")),
        ],
    )
    connector = MemoryConnector()
    client = RoutingClient(
        {
            "Table: sales.orders": AIConnectionError("orders offline"),
            "Table: sales.customers": (
                '{"short_description":"Customers","detailed_description":"master",'
                '"probable_domain":"sales","probable_role":"dimension","confidence":0.77}'
            ),
            "Column: status": (
                '{"short_description":"Status","detailed_description":"state",'
                '"probable_role":"status","confidence":0.7}'
            ),
            "Column: name": (
                '{"short_description":"Customer name","detailed_description":"display",'
                '"probable_role":"name","confidence":0.7}'
            ),
        }
    )

    SemanticEnricher(client, cache=SemanticCache(cache_dir)).enrich_schema(
        schema,
        connector,
        PrivacyMode.normal,
        parallel_workers=2,
    )

    assert schema.tables[0].semantic_short == "Semantic analysis failed"
    assert schema.tables[0].semantic_confidence == 0.0
    assert schema.tables[1].semantic_short == "Customers"
    assert schema.tables[1].columns[0].semantic_short == "Customer name"


@pytest.mark.integration
@pytest.mark.phase_9a
def test_tables_only_skips_column_enrichment(cache_dir: Path) -> None:
    schema = SchemaInfo(
        name="sales",
        engine="sqlite",
        tables=[_table("orders", _column("status"), _column("total"))],
    )
    connector = MemoryConnector()
    client = RoutingClient(
        {
            "Table: sales.orders": (
                '{"short_description":"Orders","detailed_description":"headers",'
                '"probable_domain":"sales","probable_role":"transaction_header","confidence":0.8}'
            )
        }
    )

    SemanticEnricher(client, cache=SemanticCache(cache_dir)).enrich_schema(
        schema,
        connector,
        PrivacyMode.normal,
        parallel_workers=1,
        tables_only=True,
    )

    assert schema.tables[0].semantic_short == "Orders"
    assert all(column.semantic_short is None for column in schema.tables[0].columns)


@pytest.mark.integration
@pytest.mark.phase_9a
def test_enrich_schema_can_emit_column_progress_callbacks(cache_dir: Path) -> None:
    schema = SchemaInfo(
        name="sales",
        engine="sqlite",
        tables=[_table("orders", _column("status"), _column("total"))],
    )
    connector = MemoryConnector(
        sample_rows={
            ("sales", "orders", None): [{"status": "paid", "total": "12.30"}],
            ("sales", "orders", ("status",)): [{"status": "paid"}],
            ("sales", "orders", ("total",)): [{"total": "12.30"}],
        }
    )
    client = RoutingClient(
        {
            "Table: sales.orders": (
                '{"short_description":"Orders","detailed_description":"headers",'
                '"probable_domain":"sales","probable_role":"transaction_header","confidence":0.8}'
            ),
            "Column: status": (
                '{"short_description":"Status","detailed_description":"state",'
                '"probable_role":"status","confidence":0.7}'
            ),
            "Column: total": (
                '{"short_description":"Total","detailed_description":"amount",'
                '"probable_role":"amount","confidence":0.7}'
            ),
        }
    )
    column_progress: list[tuple[str, str, int, int, int, int]] = []

    SemanticEnricher(client, cache=SemanticCache(cache_dir)).enrich_schema(
        schema,
        connector,
        PrivacyMode.normal,
        parallel_workers=1,
        on_column_complete=lambda table, column, column_index, column_total, table_index, table_total: column_progress.append(
            (table.name, column.name, column_index, column_total, table_index, table_total)
        ),
    )

    assert column_progress == [
        ("orders", "status", 1, 2, 1, 1),
        ("orders", "total", 2, 2, 1, 1),
    ]
