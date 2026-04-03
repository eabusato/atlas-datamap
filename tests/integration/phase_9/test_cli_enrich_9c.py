"""Integration tests for Phase 9C enrich CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from atlas.ai import AIConfig, LocalLLMClient, ModelInfo
from atlas.cli.enrich import enrich_cmd
from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors.base import BaseConnector
from atlas.types import (
    ColumnInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_9c]


class FakeClient(LocalLLMClient):
    """Prompt-aware local LLM stub for CLI flows."""

    def __init__(self, routes: dict[str, object], *, available: bool = True) -> None:
        super().__init__(AIConfig(provider="ollama", model="llama3"))
        self.routes = dict(routes)
        self.available = available
        self.prompts: list[str] = []

    def is_available(self) -> bool:
        return self.available

    def get_model_info(self) -> ModelInfo:
        return ModelInfo("ollama", "llama3", True, "1.0")

    def generate(
        self,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        del max_tokens, temperature
        self.prompts.append(prompt)
        prefixes = ("Column:", "Table:") if "Column:" in prompt else ("Table:", "Column:")
        for prefix in prefixes:
            for marker, response in sorted(
                ((name, value) for name, value in self.routes.items() if name.startswith(prefix)),
                key=lambda item: len(item[0]),
                reverse=True,
            ):
                if marker in prompt:
                    if isinstance(response, Exception):
                        raise response
                    return str(response)
        raise AssertionError(f"Unexpected prompt: {prompt}")


class FakeConnector(BaseConnector):
    """Connector stub for db-driven CLI integration tests."""

    def __init__(self, result: IntrospectionResult) -> None:
        config = AtlasConnectionConfig(
            engine=DatabaseEngine.sqlite,
            host="",
            database=":memory:",
            privacy_mode=PrivacyMode.normal,
        )
        super().__init__(config)
        self.result = IntrospectionResult.from_dict(result.to_dict())
        self.sample_calls: list[tuple[str, str, tuple[str, ...] | None]] = []
        self.introspected = False

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
        if columns:
            return [{columns[0]: f"{table}_{columns[0]}"}]
        return [{"sample": table}]

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        del schema, table, column
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        del schema, table, column
        return 0

    def introspect_all(self) -> IntrospectionResult:
        self.introspected = True
        return IntrospectionResult.from_dict(self.result.to_dict())


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _result() -> IntrospectionResult:
    accounts = TableInfo(
        name="accounts",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=120,
        size_bytes=8_192,
        columns=[
            ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="email", native_type="text", is_nullable=False),
        ],
    )
    orders = TableInfo(
        name="orders",
        schema="public",
        table_type=TableType.TABLE,
        row_count_estimate=320,
        size_bytes=16_384,
        columns=[
            ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
            ColumnInfo(name="account_id", native_type="integer", is_nullable=False, is_foreign_key=True),
        ],
    )
    return IntrospectionResult(
        database="atlas_cli",
        engine="sqlite",
        host="localhost",
        schemas=[SchemaInfo(name="public", engine="sqlite", tables=[accounts, orders])],
    )


def _routes(prefix: str = "") -> dict[str, str]:
    return {
        "Table: public.accounts": (
            '{"short_description":"Accounts'
            + prefix
            + '","detailed_description":"Account master","probable_domain":"crm",'
            '"probable_role":"dimension","confidence":0.91}'
        ),
        "Table: public.orders": (
            '{"short_description":"Orders'
            + prefix
            + '","detailed_description":"Order headers","probable_domain":"sales",'
            '"probable_role":"transaction_header","confidence":0.88}'
        ),
        "Column: id": (
            '{"short_description":"Identifier","detailed_description":"Technical id",'
            '"probable_role":"identifier","confidence":0.95}'
        ),
        "Column: email": (
            '{"short_description":"Email","detailed_description":"Primary email",'
            '"probable_role":"email","confidence":0.96}'
        ),
        "Column: account_id": (
            '{"short_description":"Account reference","detailed_description":"FK to account",'
            '"probable_role":"foreign_key","confidence":0.9}'
        ),
    }


def _write_sigil(path: Path, result: IntrospectionResult) -> None:
    path.write_text(result.to_json(indent=2), encoding="utf-8")


def test_enrich_fails_without_input_source(runner: CliRunner) -> None:
    result = runner.invoke(enrich_cmd, [])
    assert result.exit_code != 0
    assert "Provide exactly one structural source" in result.output


def test_enrich_fails_with_multiple_input_sources(
    runner: CliRunner,
    phase_tmp_dir: Path,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    _write_sigil(sigil_path, _result())

    result = runner.invoke(enrich_cmd, ["--sigil", str(sigil_path), "--db", "sqlite:///:memory:"])
    assert result.exit_code != 0
    assert "Provide exactly one structural source" in result.output


def test_enrich_aborts_when_local_ai_is_offline(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    _write_sigil(sigil_path, _result())
    monkeypatch.setattr("atlas.cli.enrich.build_client", lambda config: FakeClient({}, available=False))

    result = runner.invoke(enrich_cmd, ["--sigil", str(sigil_path), "--output", str(phase_tmp_dir / "out")])
    assert result.exit_code != 0
    assert "Local AI provider is unavailable" in result.output


def test_enrich_from_sigil_writes_enriched_artifacts(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    out_dir = phase_tmp_dir / "out"
    _write_sigil(sigil_path, _result())
    client = FakeClient(_routes())
    monkeypatch.setattr("atlas.cli.enrich.build_client", lambda config: client)
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    result = runner.invoke(enrich_cmd, ["--sigil", str(sigil_path), "--output", str(out_dir)])
    assert result.exit_code == 0, result.output

    semantic_sigil = out_dir / "atlas_semantic.sigil"
    semantic_svg = out_dir / "atlas_semantic.svg"
    semantic_meta = out_dir / "atlas_semantic_meta.json"
    assert semantic_sigil.exists()
    assert semantic_svg.exists()
    assert semantic_meta.exists()
    assert 'data-semantic-short="Accounts"' in semantic_svg.read_text(encoding="utf-8")

    enriched = IntrospectionResult.from_json(semantic_sigil.read_text(encoding="utf-8"))
    assert enriched.get_table("public", "accounts").semantic_short == "Accounts"
    assert enriched.get_table("public", "accounts").columns[1].semantic_short == "Email"
    assert enriched.get_table("public", "accounts").columns[1].semantic_role == "email"
    assert any("Column:" in prompt for prompt in client.prompts)
    assert "column 1/2" in result.output
    assert "table 1/2" in result.output


def test_enrich_full_column_mode_runs_column_prompts(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    out_dir = phase_tmp_dir / "out"
    _write_sigil(sigil_path, _result())
    client = FakeClient(_routes())
    monkeypatch.setattr("atlas.cli.enrich.build_client", lambda config: client)
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    result = runner.invoke(
        enrich_cmd,
        ["--sigil", str(sigil_path), "--output", str(out_dir), "--column-mode", "full"],
    )
    assert result.exit_code == 0, result.output

    enriched = IntrospectionResult.from_json((out_dir / "atlas_semantic.sigil").read_text(encoding="utf-8"))
    assert enriched.get_table("public", "accounts").columns[1].semantic_short == "Email"
    assert any("Column:" in prompt for prompt in client.prompts)
    assert "column 1/2" in result.output


def test_enrich_from_db_uses_connector_and_writes_artifacts(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_connector = FakeConnector(_result())
    out_dir = phase_tmp_dir / "out"
    monkeypatch.setattr("atlas.cli.enrich.build_client", lambda config: FakeClient(_routes()))
    monkeypatch.setattr("atlas.cli.enrich.resolve_config", lambda **kwargs: fake_connector.config)
    monkeypatch.setattr("atlas.cli.enrich.get_connector", lambda config: fake_connector)
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    result = runner.invoke(
        enrich_cmd,
        ["--db", "sqlite:///:memory:", "--output", str(out_dir), "--schema", "public"],
    )
    assert result.exit_code == 0, result.output
    assert fake_connector.introspected is True
    assert fake_connector.sample_calls
    assert (out_dir / "atlas_cli_semantic.svg").exists()


def test_enrich_dry_run_skips_llm_and_writes_nothing(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    out_dir = phase_tmp_dir / "out"
    _write_sigil(sigil_path, _result())
    monkeypatch.setattr(
        "atlas.cli.enrich.build_client",
        lambda config: (_ for _ in ()).throw(AssertionError("LLM must not be initialized in dry-run")),
    )

    result = runner.invoke(
        enrich_cmd,
        ["--sigil", str(sigil_path), "--output", str(out_dir), "--dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert "Dry run" in result.output
    assert "Column mode=full" in result.output
    assert not out_dir.exists()


def test_enrich_requires_schema_when_table_filter_is_used(
    runner: CliRunner,
    phase_tmp_dir: Path,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    _write_sigil(sigil_path, _result())

    result = runner.invoke(enrich_cmd, ["--sigil", str(sigil_path), "--table", "orders"])
    assert result.exit_code != 0
    assert "--table requires --schema" in result.output


def test_enrich_tables_only_skips_column_semantics(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    out_dir = phase_tmp_dir / "out"
    _write_sigil(sigil_path, _result())
    monkeypatch.setattr(
        "atlas.cli.enrich.build_client",
        lambda config: FakeClient(
            {
                "Table: public.accounts": (
                    '{"short_description":"Accounts","detailed_description":"Account master",'
                    '"probable_domain":"crm","probable_role":"dimension","confidence":0.91}'
                ),
                "Table: public.orders": (
                    '{"short_description":"Orders","detailed_description":"Order headers",'
                    '"probable_domain":"sales","probable_role":"transaction_header","confidence":0.88}'
                ),
            }
        ),
    )
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    result = runner.invoke(
        enrich_cmd,
        ["--sigil", str(sigil_path), "--output", str(out_dir), "--tables-only"],
    )
    assert result.exit_code == 0, result.output

    enriched = IntrospectionResult.from_json((out_dir / "atlas_semantic.sigil").read_text(encoding="utf-8"))
    assert enriched.get_table("public", "accounts").semantic_short == "Accounts"
    assert all(column.semantic_short is None for column in enriched.get_table("public", "accounts").columns)


def test_enrich_accepts_separate_ai_config_and_selection_file(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    out_dir = phase_tmp_dir / "out"
    ai_config_path = phase_tmp_dir / "atlas.ai.toml"
    selection_path = phase_tmp_dir / "selection.json"
    _write_sigil(sigil_path, _result())
    ai_config_path.write_text(
        "[ai]\nprovider='ollama'\nmodel='qwen2.5:1.5b'\nbase_url='http://127.0.0.1:11434'\n",
        encoding="utf-8",
    )
    selection_path.write_text(
        '{"schemas":["public"],"tables":{"public":["accounts"]},"columns":{"public.accounts":["email"]}}',
        encoding="utf-8",
    )
    client = FakeClient(_routes())
    monkeypatch.setattr("atlas.cli.enrich.build_client", lambda config: client)
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    result = runner.invoke(
        enrich_cmd,
        [
            "--sigil",
            str(sigil_path),
            "--ai-config",
            str(ai_config_path),
            "--selection",
            str(selection_path),
            "--output",
            str(out_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    enriched = IntrospectionResult.from_json((out_dir / "atlas_semantic.sigil").read_text(encoding="utf-8"))
    assert enriched.get_table("public", "accounts") is not None
    assert enriched.get_table("public", "orders") is None
    account_columns = enriched.get_table("public", "accounts").columns
    assert next(column for column in account_columns if column.name == "email").semantic_role == "email"
    assert next(column for column in account_columns if column.name == "id").semantic_short is None


def test_enrich_force_ignores_existing_cache(
    runner: CliRunner,
    phase_tmp_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sigil_path = phase_tmp_dir / "atlas.sigil"
    out_dir = phase_tmp_dir / "out"
    _write_sigil(sigil_path, _result())
    monkeypatch.setattr("atlas.sigilo.builder._sigilo.available", lambda: False)

    monkeypatch.setattr("atlas.cli.enrich.build_client", lambda config: FakeClient(_routes()))
    first = runner.invoke(enrich_cmd, ["--sigil", str(sigil_path), "--output", str(out_dir)])
    assert first.exit_code == 0, first.output

    monkeypatch.setattr(
        "atlas.cli.enrich.build_client",
        lambda config: FakeClient(_routes(prefix=" v2")),
    )
    second = runner.invoke(
        enrich_cmd,
        ["--sigil", str(sigil_path), "--output", str(out_dir), "--force"],
    )
    assert second.exit_code == 0, second.output

    enriched = IntrospectionResult.from_json((out_dir / "atlas_semantic.sigil").read_text(encoding="utf-8"))
    assert enriched.get_table("public", "accounts").semantic_short == "Accounts v2"
