"""Phase 5A unit tests for introspection orchestration and scan CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from atlas.cli.scan import scan_cmd
from atlas.config import AtlasConnectionConfig, DatabaseEngine, PrivacyMode
from atlas.connectors.base import BaseConnector
from atlas.export.snapshot import artifact_paths, sanitize_stem, save_artifacts
from atlas.introspection.runner import IntrospectionError, IntrospectionRunner, _ProgressEvent
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)


def _config(**overrides: object) -> AtlasConnectionConfig:
    payload: dict[str, object] = {
        "engine": DatabaseEngine.postgresql,
        "host": "localhost",
        "database": "Atlas São Paulo/Main DB",
    }
    payload.update(overrides)
    return AtlasConnectionConfig(**payload)


class _RunnerConnector(BaseConnector):
    def __init__(self, config: AtlasConnectionConfig) -> None:
        super().__init__(config)
        self.connected_calls = 0
        self.disconnected_calls = 0

    def connect(self) -> None:
        self.connected_calls += 1
        self._connected = True

    def disconnect(self) -> None:
        self.disconnected_calls += 1
        self._connected = False

    def get_schemas(self) -> list[SchemaInfo]:
        return [
            SchemaInfo(name="public", engine="postgresql"),
            SchemaInfo(name="audit", engine="postgresql"),
        ]

    def get_tables(self, schema: str) -> list[TableInfo]:
        if schema == "public":
            return [
                TableInfo(name="customers", schema=schema, table_type=TableType.TABLE),
                TableInfo(name="orders", schema=schema, table_type=TableType.TABLE),
            ]
        return [TableInfo(name="events", schema=schema, table_type=TableType.TABLE)]

    def get_row_count_estimate(self, schema: str, table: str) -> int:
        return {("public", "customers"): 3, ("public", "orders"): 5, ("audit", "events"): 1}[schema, table]

    def get_table_size_bytes(self, schema: str, table: str) -> int:
        return {("public", "customers"): 128, ("public", "orders"): 512, ("audit", "events"): 64}[schema, table]

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        if table == "customers":
            return [
                ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
                ColumnInfo(name="email", native_type="text", is_nullable=False),
            ]
        if table == "orders":
            return [
                ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False),
                ColumnInfo(name="customer_id", native_type="integer", is_nullable=False),
            ]
        return [ColumnInfo(name="id", native_type="integer", is_primary_key=True, is_nullable=False)]

    def get_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyInfo]:
        if table != "orders":
            return []
        return [
            ForeignKeyInfo(
                name="fk_orders_customer",
                source_schema=schema,
                source_table=table,
                source_columns=["customer_id"],
                target_schema="public",
                target_table="customers",
                target_columns=["id"],
            )
        ]

    def get_indexes(self, schema: str, table: str) -> list[Any]:
        return []

    def get_sample_rows(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
        privacy_mode: PrivacyMode | None = None,
    ) -> list[dict[str, Any]]:
        return []

    def get_column_null_count(self, schema: str, table: str, column: str) -> int:
        return 0

    def get_column_distinct_estimate(self, schema: str, table: str, column: str) -> int:
        return 0


class _ExplodingConnector(_RunnerConnector):
    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        if table == "orders":
            raise RuntimeError("boom")
        return super().get_columns(schema, table)


def test_runner_builds_result_and_fk_in_degree() -> None:
    connector = _RunnerConnector(_config())

    result = IntrospectionRunner(connector.config, connector).run()

    assert connector.connected_calls == 1
    assert connector.disconnected_calls == 1
    assert result.total_tables == 3
    assert result.get_table("public", "customers") is not None
    assert result.get_table("public", "customers").fk_in_degree == 1


def test_runner_applies_schema_filter() -> None:
    config = _config(schema_filter=["audit"])
    connector = _RunnerConnector(config)

    result = IntrospectionRunner(config, connector).run()

    assert [schema.name for schema in result.schemas] == ["audit"]


def test_runner_wraps_table_failures_with_context() -> None:
    connector = _ExplodingConnector(_config())

    with pytest.raises(IntrospectionError, match="public.orders"):
        IntrospectionRunner(connector.config, connector).run()


def test_runner_emits_progress_events() -> None:
    connector = _RunnerConnector(_config())
    events: list[_ProgressEvent] = []

    IntrospectionRunner(connector.config, connector, on_progress=events.append).run()

    stages = {event.stage for event in events}
    assert {"connect", "schemas", "tables", "columns", "relations"} <= stages


def test_save_artifacts_writes_svg_sigil_and_meta(tmp_path: Path) -> None:
    result = IntrospectionResult(
        database="atlas",
        engine="sqlite",
        host="",
        schemas=[SchemaInfo(name="main", engine="sqlite", tables=[TableInfo(name="users", schema="main")])],
    )

    artifacts = save_artifacts(result, b"<svg></svg>", tmp_path)

    assert artifacts.svg_path.read_bytes() == b"<svg></svg>"
    assert artifacts.meta_json_path.read_text(encoding="utf-8").startswith("{\n  ")
    assert artifacts.sigil_path.read_text(encoding="utf-8") == json.dumps(
        result.to_dict(),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def test_sanitize_stem_normalizes_ascii_and_truncates() -> None:
    stem = sanitize_stem("São/Paulo: Atlas Main Database " + ("x" * 90))

    assert stem.startswith("Sao_Paulo_Atlas_Main_Database")
    assert len(stem) <= 64
    assert "/" not in stem
    assert ":" not in stem


def test_artifact_paths_use_sanitized_database_name(tmp_path: Path) -> None:
    result = IntrospectionResult(database="São Paulo/Main", engine="sqlite", host="")

    paths = artifact_paths(result, tmp_path)

    assert paths.svg_path.name == "Sao_Paulo_Main.svg"
    assert paths.meta_json_path.name == "Sao_Paulo_Main_meta.json"
    assert paths.sigil_path.name == "Sao_Paulo_Main.sigil"


def test_scan_cli_generates_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured_config: AtlasConnectionConfig | None = None

    class _ScanConnector(_RunnerConnector):
        pass

    def _fake_get_connector(config: AtlasConnectionConfig) -> BaseConnector:
        nonlocal captured_config
        captured_config = config
        return _ScanConnector(config)

    monkeypatch.setattr("atlas.cli.scan.get_connector", _fake_get_connector)
    monkeypatch.setattr("atlas.cli.scan._build_svg", lambda *args, **kwargs: b"<svg></svg>")

    runner = CliRunner()
    result = runner.invoke(
        scan_cmd,
        [
            "--db",
            "postgresql://localhost/atlas",
            "--schema",
            "public, audit",
            "--privacy",
            "masked",
            "--output",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Scan completed for atlas" in result.output
    assert (tmp_path / "atlas.svg").exists()
    assert captured_config is not None
    assert captured_config.schema_filter == ["public", "audit"]
    assert captured_config.privacy_mode is PrivacyMode.masked


def test_scan_cli_dry_run_skips_writes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("atlas.cli.scan.get_connector", lambda config: _RunnerConnector(config))
    monkeypatch.setattr("atlas.cli.scan._build_svg", lambda *args, **kwargs: b"<svg></svg>")

    runner = CliRunner()
    result = runner.invoke(
        scan_cmd,
        ["--db", "postgresql://localhost/atlas", "--output", str(tmp_path), "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "Dry run completed" in result.output
    assert list(tmp_path.iterdir()) == []


def test_scan_cli_dry_run_skips_svg_rendering(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("atlas.cli.scan.get_connector", lambda config: _RunnerConnector(config))
    monkeypatch.setattr(
        "atlas.cli.scan._build_svg",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("render should be skipped")),
    )

    runner = CliRunner()
    result = runner.invoke(
        scan_cmd,
        ["--db", "postgresql://localhost/atlas", "--output", str(tmp_path), "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "Dry run completed" in result.output


def test_scan_cli_warns_when_generic_connector_is_used(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    generic_config = AtlasConnectionConfig(
        engine=DatabaseEngine.generic,
        host="",
        database="atlas",
        connect_args={"sqlalchemy_url": "sqlite:////tmp/atlas.db"},
    )
    monkeypatch.setattr("atlas.cli.scan.resolve_config", lambda **kwargs: generic_config)
    monkeypatch.setattr("atlas.cli.scan.get_connector", lambda config: _RunnerConnector(config))

    runner = CliRunner()
    result = runner.invoke(
        scan_cmd,
        ["--db", "generic+sqlite:////tmp/atlas.db", "--output", str(tmp_path), "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "Using generic connector" in result.output


def test_scan_cli_requires_force_to_overwrite(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("atlas.cli.scan.get_connector", lambda config: _RunnerConnector(config))
    monkeypatch.setattr("atlas.cli.scan._build_svg", lambda *args, **kwargs: b"<svg></svg>")
    (tmp_path / "atlas.svg").write_text("existing", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        scan_cmd,
        ["--db", "postgresql://localhost/atlas", "--output", str(tmp_path)],
    )

    assert result.exit_code != 0
    assert "--force" in result.output


def test_scan_cli_quiet_suppresses_progress(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("atlas.cli.scan.get_connector", lambda config: _RunnerConnector(config))
    monkeypatch.setattr("atlas.cli.scan._build_svg", lambda *args, **kwargs: b"<svg></svg>")

    runner = CliRunner()
    result = runner.invoke(
        scan_cmd,
        ["--db", "postgresql://localhost/atlas", "--output", str(tmp_path), "--quiet"],
    )

    assert result.exit_code == 0, result.output
    assert "[atlas scan] connect" not in result.output
    assert "Scan completed for atlas" in result.output
