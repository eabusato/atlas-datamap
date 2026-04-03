"""Interactive onboarding workflow for full Atlas runs."""

from __future__ import annotations

import contextlib
import json
import os
import stat
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

from atlas.ai import AIConfig, SemanticCache, SemanticEnricher, build_client
from atlas.config import AtlasConnectionConfig, ConfigValidationError, DatabaseEngine, PrivacyMode
from atlas.export import (
    AtlasSnapshot,
    ExecutiveReportGenerator,
    HTMLReportGenerator,
    SnapshotDiff,
    SnapshotDiffReport,
    StandaloneHTMLBuilder,
    StructuredExporter,
    sanitize_stem,
)
from atlas.history import AtlasHistory
from atlas.introspection.runner import _ProgressEvent
from atlas.sdk import Atlas
from atlas.sigilo.panel import PanelBuilder
from atlas.types import SchemaInfo

_LOCAL_AI_HOSTS = {"localhost", "127.0.0.1", "::1"}
_MANIFEST_VERSION = 1
OnboardingProgressCallback = Callable[[str], None]


@dataclass(slots=True)
class DatabaseSetup:
    """Local onboarding database connection contract."""

    engine: str
    host: str = ""
    port: int | None = None
    database: str = ""
    sqlite_path: str | None = None
    ssl_mode: str = "disable"
    timeout_seconds: int = 30
    sample_limit: int = 50
    privacy_mode: str = PrivacyMode.masked.value
    schema_filter: list[str] = field(default_factory=list)
    schema_exclude: list[str] = field(default_factory=list)
    connect_args: dict[str, str] = field(default_factory=dict)
    user_env_var: str | None = None
    password_env_var: str | None = None
    url_env_var: str | None = None

    def build_config(self, secrets: dict[str, str]) -> AtlasConnectionConfig:
        """Resolve an AtlasConnectionConfig from local manifest + env-backed secrets."""

        privacy_mode = PrivacyMode(self.privacy_mode)
        overrides: dict[str, Any] = {
            "ssl_mode": self.ssl_mode,
            "timeout_seconds": self.timeout_seconds,
            "schema_filter": list(self.schema_filter),
            "schema_exclude": list(self.schema_exclude),
            "sample_limit": self.sample_limit,
            "privacy_mode": privacy_mode,
            "connect_args": dict(self.connect_args),
        }

        if self.url_env_var:
            url = secrets.get(self.url_env_var, "").strip()
            if not url:
                raise ConfigValidationError(
                    f"Missing database URL in env var {self.url_env_var!r}."
                )
            return AtlasConnectionConfig.from_url(url, **overrides)

        engine = DatabaseEngine(self.engine)
        if engine is DatabaseEngine.sqlite:
            database = (self.sqlite_path or self.database).strip()
            return AtlasConnectionConfig(
                engine=engine,
                host="",
                database=database,
                **overrides,
            )

        return AtlasConnectionConfig(
            engine=engine,
            host=self.host.strip(),
            port=self.port,
            database=self.database.strip(),
            user=secrets.get(self.user_env_var or "", "").strip() or None,
            password=secrets.get(self.password_env_var or "", "").strip() or None,
            **overrides,
        )


@dataclass(slots=True)
class AISetup:
    """Local AI configuration captured by the onboarding wizard."""

    enabled: bool = False
    provider: str = "ollama"
    model: str = "qwen2.5:1.5b"
    base_url: str = "http://127.0.0.1:11434"
    api_key_env_var: str | None = None
    temperature: float = 0.1
    max_tokens: int = 300
    timeout_seconds: float = 60.0
    parallel_workers: int = 2
    column_mode: str = "full"
    force_recompute: bool = False
    selection_schemas: list[str] = field(default_factory=list)
    selection_tables: list[str] = field(default_factory=list)
    selection_columns: list[str] = field(default_factory=list)

    def build_config(self, secrets: dict[str, str]) -> AIConfig | None:
        """Resolve a concrete AIConfig from locally stored secret refs."""

        if not self.enabled:
            return None
        host = (urlparse(self.base_url).hostname or "").lower()
        if host not in _LOCAL_AI_HOSTS:
            raise ValueError(
                "Atlas onboarding only allows local AI endpoints so metadata never leaves "
                "the user's environment. Use localhost or 127.0.0.1."
            )
        api_key = None
        if self.api_key_env_var:
            api_key = secrets.get(self.api_key_env_var, "").strip() or None
        return AIConfig(
            provider=self.provider,
            model=self.model,
            base_url=self.base_url,
            api_key=api_key,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            timeout_seconds=self.timeout_seconds,
        )


@dataclass(slots=True)
class OnboardingManifest:
    """Persistent local-only onboarding manifest."""

    project_name: str
    workspace_dir: str
    generated_dir_name: str = "generated"
    sigilo_style: str = "network"
    sigilo_layout: str = "circular"
    database: DatabaseSetup = field(default_factory=lambda: DatabaseSetup(engine="sqlite"))
    ai: AISetup = field(default_factory=AISetup)
    env_path: str = ".env"
    managed_env: bool = True
    manifest_version: int = _MANIFEST_VERSION

    @property
    def workspace_path(self) -> Path:
        return Path(self.workspace_dir)

    @property
    def generated_path(self) -> Path:
        return self.workspace_path / self.generated_dir_name

    @property
    def manifest_path(self) -> Path:
        return self.workspace_path / "atlas.onboard.json"

    @property
    def env_file_path(self) -> Path:
        env_path = Path(self.env_path)
        if env_path.is_absolute():
            return env_path
        return self.workspace_path / env_path

    @property
    def connection_reference_path(self) -> Path:
        return self.workspace_path / "atlas.connection.toml"

    @property
    def ai_reference_path(self) -> Path:
        return self.workspace_path / "atlas.ai.toml"

    @property
    def selection_path(self) -> Path:
        return self.workspace_path / "atlas.selection.json"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> OnboardingManifest:
        return cls(
            project_name=str(payload.get("project_name", "Atlas Onboarding Run")),
            workspace_dir=str(payload.get("workspace_dir", ".")),
            generated_dir_name=str(payload.get("generated_dir_name", "generated")),
            sigilo_style=str(payload.get("sigilo_style", "network")),
            sigilo_layout=str(payload.get("sigilo_layout", "circular")),
            database=DatabaseSetup(**dict(payload.get("database", {}))),
            ai=AISetup(**dict(payload.get("ai", {}))),
            env_path=str(payload.get("env_path", ".env")),
            managed_env=bool(payload.get("managed_env", True)),
            manifest_version=int(payload.get("manifest_version", _MANIFEST_VERSION)),
        )

    @classmethod
    def load(cls, path: str | Path) -> OnboardingManifest:
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    def save(self) -> Path:
        self.workspace_path.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return self.manifest_path


@dataclass(slots=True)
class OnboardingPaths:
    """Filesystem layout for a full onboarding run."""

    workspace: Path
    generated: Path
    scans: Path
    semantic: Path
    exports: Path
    reports: Path
    history: Path
    diff: Path


@dataclass(slots=True)
class OnboardingOutputs:
    """Summary of files emitted by a successful onboarding run."""

    workspace: Path
    manifest_path: Path
    env_path: Path
    scan_svg: Path
    scan_sigil: Path
    scan_meta: Path
    scan_panel_html: Path
    scan_snapshot: Path
    standalone_html: Path
    health_report: Path
    executive_report: Path
    dictionary_json: Path
    tables_csv: Path
    columns_csv: Path
    dictionary_md: Path
    history_snapshot: Path
    diff_report: Path | None = None
    semantic_svg: Path | None = None
    semantic_sigil: Path | None = None
    semantic_meta: Path | None = None
    semantic_panel_html: Path | None = None
    semantic_snapshot: Path | None = None
    connection_reference: Path | None = None
    ai_reference: Path | None = None
    selection_json: Path | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            key: (str(value) if isinstance(value, Path) else value)
            for key, value in asdict(self).items()
        }
        return payload


@dataclass(slots=True)
class SelectionPlan:
    """Resolved schema/table/column selectors for semantic enrichment."""

    schema_names: set[str] | None
    tables_by_schema: dict[str, set[str]]
    columns_by_table: dict[tuple[str, str], set[str] | None]


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _ensure_local_ai_base_url(base_url: str) -> None:
    host = (urlparse(base_url).hostname or "").lower()
    if host not in _LOCAL_AI_HOSTS:
        raise ValueError(
            "Atlas onboarding only allows local AI endpoints so metadata and secrets "
            "stay on the user's machine."
        )


def _quote_toml(value: str) -> str:
    return json.dumps(value)


def _toml_list(values: list[str]) -> str:
    return "[" + ", ".join(_quote_toml(item) for item in values) + "]"


def _write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _write_json(path: Path, payload: Any) -> Path:
    return _write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


def _write_env_file(path: Path, values: dict[str, str]) -> Path:
    body = "".join(f"{key}={json.dumps(value)}\n" for key, value in values.items())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    with os.fdopen(os.open(path, os.O_RDONLY), "r", encoding="utf-8"):
        pass
    with contextlib.suppress(OSError):
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = raw_value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _load_secret_values(manifest: OnboardingManifest) -> dict[str, str]:
    values = dict(os.environ)
    values.update(_parse_env_file(manifest.env_file_path))
    return values


def _build_selection_plan(ai: AISetup) -> SelectionPlan | None:
    schema_names = set(ai.selection_schemas) or None
    tables_by_schema: dict[str, set[str]] = {}
    columns_by_table: dict[tuple[str, str], set[str] | None] = {}

    for table_ref in ai.selection_tables:
        schema_name, table_name = _parse_table_ref(table_ref)
        tables_by_schema.setdefault(schema_name, set()).add(table_name)

    for column_ref in ai.selection_columns:
        schema_name, table_name, column_name = _parse_column_ref(column_ref)
        tables_by_schema.setdefault(schema_name, set()).add(table_name)
        key = (schema_name, table_name)
        selected_columns = columns_by_table.get(key)
        if selected_columns is None:
            selected_columns = set()
            columns_by_table[key] = selected_columns
        selected_columns.add(column_name)

    if schema_names is None and tables_by_schema:
        schema_names = set(tables_by_schema)
    elif schema_names is not None:
        schema_names.update(tables_by_schema)

    if schema_names is None and not tables_by_schema and not columns_by_table:
        return None
    return SelectionPlan(
        schema_names=schema_names,
        tables_by_schema=tables_by_schema,
        columns_by_table=columns_by_table,
    )


def _parse_table_ref(value: str) -> tuple[str, str]:
    schema_name, separator, table_name = value.strip().partition(".")
    if not separator or not schema_name or not table_name:
        raise ValueError(f"Table selector {value!r} must use 'schema.table'.")
    return schema_name.strip(), table_name.strip()


def _parse_column_ref(value: str) -> tuple[str, str, str]:
    parts = [item.strip() for item in value.strip().split(".", 2)]
    if len(parts) != 3 or not all(parts):
        raise ValueError(
            f"Column selector {value!r} must use 'schema.table.column'."
        )
    return parts[0], parts[1], parts[2]


def _build_paths(manifest: OnboardingManifest) -> OnboardingPaths:
    generated = manifest.generated_path
    return OnboardingPaths(
        workspace=manifest.workspace_path,
        generated=generated,
        scans=generated / "scans",
        semantic=generated / "semantic",
        exports=generated / "exports",
        reports=generated / "reports",
        history=generated / "history",
        diff=generated / "diff",
    )


def _emit_progress(
    callback: OnboardingProgressCallback | None,
    message: str,
) -> None:
    if callback is not None:
        callback(message)


def _render_scan_progress(event: _ProgressEvent) -> str:
    prefix = f"[atlas onboard] scan {event.stage}"
    if event.total > 0 and event.current > 0:
        prefix += f" {event.current}/{event.total}"
    prefix += f" [{event.elapsed_ms} ms]"
    return f"{prefix} {event.message}"


def _format_elapsed(started_at: float) -> str:
    return f"{int((time.perf_counter() - started_at) * 1000)} ms"


def _write_reference_files(
    manifest: OnboardingManifest,
    *,
    include_ai: bool,
    selection_plan: SelectionPlan | None,
) -> tuple[Path, Path | None, Path | None]:
    db = manifest.database
    env_reference = os.path.relpath(manifest.env_file_path, manifest.connection_reference_path.parent)
    connection_toml = [
        "# Atlas onboarding reference file",
        "# Secrets are resolved locally from the env file referenced by atlas.onboard.json.",
        "[connection]",
        f"engine = {_quote_toml(db.engine)}",
        f"env_file = {_quote_toml(env_reference)}",
    ]
    if db.url_env_var:
        connection_toml.append(f"url_env = {_quote_toml(db.url_env_var)}")
    elif DatabaseEngine(db.engine) is DatabaseEngine.sqlite:
        connection_toml.append(
            f"database = {_quote_toml((db.sqlite_path or db.database).strip())}"
        )
    else:
        connection_toml.extend(
            [
                f"host = {_quote_toml(db.host)}",
                f"database = {_quote_toml(db.database)}",
                f"port = {db.port if db.port is not None else '0'}",
                f"user_env = {_quote_toml(db.user_env_var or '')}",
                f"password_env = {_quote_toml(db.password_env_var or '')}",
                f"ssl_mode = {_quote_toml(db.ssl_mode)}",
            ]
        )
    connection_toml.extend(
        [
            "",
            "[analysis]",
            f"timeout_seconds = {db.timeout_seconds}",
            f"sample_limit = {db.sample_limit}",
            f"privacy_mode = {_quote_toml(db.privacy_mode)}",
            f"schema_filter = {_toml_list(db.schema_filter)}",
            f"schema_exclude = {_toml_list(db.schema_exclude)}",
        ]
    )
    connection_path = _write_text(
        manifest.connection_reference_path,
        "\n".join(connection_toml) + "\n",
    )

    ai_path: Path | None = None
    if include_ai:
        ai = manifest.ai
        ai_lines = [
            "# Atlas onboarding AI reference file",
            "# This onboarding flow only allows local AI endpoints.",
            "[ai]",
            f"provider = {_quote_toml(ai.provider)}",
            f"model = {_quote_toml(ai.model)}",
            f"base_url = {_quote_toml(ai.base_url)}",
            f"temperature = {ai.temperature}",
            f"max_tokens = {ai.max_tokens}",
            f"timeout_seconds = {ai.timeout_seconds}",
        ]
        if ai.api_key_env_var:
            ai_lines.append(f"api_key_env = {_quote_toml(ai.api_key_env_var)}")
        ai_path = _write_text(manifest.ai_reference_path, "\n".join(ai_lines) + "\n")

    selection_path: Path | None = None
    if selection_plan is not None:
        selection_payload: dict[str, Any] = {}
        if selection_plan.schema_names is not None:
            selection_payload["schemas"] = sorted(selection_plan.schema_names)
        if selection_plan.tables_by_schema:
            selection_payload["tables"] = {
                schema_name: sorted(table_names)
                for schema_name, table_names in sorted(selection_plan.tables_by_schema.items())
            }
        if selection_plan.columns_by_table:
            selection_payload["columns"] = {
                f"{schema_name}.{table_name}": sorted(column_names or [])
                for (schema_name, table_name), column_names in sorted(
                    selection_plan.columns_by_table.items()
                )
            }
        selection_path = _write_json(manifest.selection_path, selection_payload)

    return connection_path, ai_path, selection_path


def _build_subset_schema(schema: SchemaInfo, table_names: set[str] | None) -> SchemaInfo | None:
    tables = [
        table for table in schema.tables if table_names is None or table.name in table_names
    ]
    if not tables:
        return None
    return SchemaInfo(
        name=schema.name,
        engine=schema.engine,
        tables=tables,
        table_count=sum(1 for table in tables if table.table_type.value == "table"),
        view_count=sum(1 for table in tables if table.table_type.value != "table"),
        total_size_bytes=sum(table.size_bytes for table in tables),
        introspected_at=schema.introspected_at,
        extra_metadata=dict(schema.extra_metadata),
    )


def _run_semantic_enrichment(
    atlas: Atlas,
    result: Any,
    manifest: OnboardingManifest,
    ai_config: AIConfig,
    paths: OnboardingPaths,
    *,
    on_progress: OnboardingProgressCallback | None = None,
) -> tuple[Any, bytes, AtlasSnapshot, Path, Path, Path, Path]:
    client = build_client(ai_config)
    if not client.is_available():
        raise RuntimeError(
            "Configured local AI provider is unavailable. Start it and rerun atlas onboard."
        )
    model_info = client.get_model_info()
    _emit_progress(
        on_progress,
        (
            "[atlas onboard] semantic ai "
            f"Using local model {model_info.model_name} via {model_info.provider_name}."
        ),
    )

    cache = SemanticCache(paths.semantic / ".semantic_cache")
    enricher = SemanticEnricher(client, cache=cache)
    selection = _build_selection_plan(manifest.ai)

    connector = atlas.connector
    with connector.session():
        for schema in result.schemas:
            if (
                selection is not None
                and selection.schema_names is not None
                and schema.name not in selection.schema_names
            ):
                continue
            schema_subset = _build_subset_schema(
                schema,
                selection.tables_by_schema.get(schema.name) if selection is not None else None,
            )
            if schema_subset is None:
                continue
            _emit_progress(
                on_progress,
                (
                    "[atlas onboard] semantic schema "
                    f"Enriching schema {schema_subset.name} ({len(schema_subset.tables)} tables, "
                    f"column mode={manifest.ai.column_mode})."
                ),
            )
            enricher.enrich_schema(
                schema_subset,
                connector,
                atlas.config.privacy_mode,
                parallel_workers=manifest.ai.parallel_workers,
                force_recompute=manifest.ai.force_recompute,
                column_mode=cast(
                    Literal["infer", "full", "skip"],
                    manifest.ai.column_mode,
                ),
                selected_columns_by_table=(
                    selection.columns_by_table if selection is not None else None
                ),
                on_table_complete=(
                    lambda finished_table, current, total: _emit_progress(
                        on_progress,
                        (
                            "[atlas onboard] semantic table "
                            f"{current}/{total}: {finished_table.qualified_name} "
                            f"({len(finished_table.columns)} columns)"
                        ),
                    )
                ),
                on_column_complete=(
                    lambda finished_table, column, column_index, column_total, table_index, table_total: _emit_progress(
                        on_progress,
                        (
                            "[atlas onboard] semantic column "
                            f"table {table_index}/{table_total}: "
                            f"{finished_table.qualified_name}.{column.name} "
                            f"({column_index}/{column_total})"
                        ),
                    )
                ),
            )

    semantic_sigilo = atlas.build_sigilo(
        result,
        style=cast(Literal["network", "seal", "compact"], manifest.sigilo_style),
        layout=cast(Literal["circular", "force"], manifest.sigilo_layout),
        schema_filter=manifest.database.schema_filter or None,
    )
    semantic_stem = f"{sanitize_stem(result.database)}_semantic"
    semantic_svg = paths.semantic / f"{semantic_stem}.svg"
    semantic_sigil = paths.semantic / f"{semantic_stem}.sigil"
    semantic_meta = paths.semantic / f"{semantic_stem}_meta.json"
    semantic_panel = paths.semantic / f"{semantic_stem}_panel.html"
    semantic_snapshot_path = paths.semantic / f"{semantic_stem}.atlas"
    paths.semantic.mkdir(parents=True, exist_ok=True)
    semantic_svg.write_bytes(semantic_sigilo.svg_bytes)
    semantic_sigil.write_text(
        json.dumps(result.to_dict(), ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    semantic_meta.write_text(result.to_json(indent=2), encoding="utf-8")
    semantic_panel.write_text(
        PanelBuilder(semantic_sigilo.svg_bytes, db_name=result.database).build_html(),
        encoding="utf-8",
    )
    semantic_snapshot = atlas.create_snapshot(result, semantic_sigilo)
    semantic_snapshot.save(semantic_snapshot_path)
    _emit_progress(
        on_progress,
        f"[atlas onboard] semantic save Wrote semantic artifacts under {paths.semantic}",
    )
    return (
        result,
        semantic_sigilo.svg_bytes,
        semantic_snapshot,
        semantic_svg,
        semantic_sigil,
        semantic_meta,
        semantic_panel,
    )


def run_onboarding(
    manifest: OnboardingManifest,
    *,
    on_progress: OnboardingProgressCallback | None = None,
) -> OnboardingOutputs:
    """Run a full Atlas pipeline from one local-only onboarding manifest."""

    started_at = time.perf_counter()
    _emit_progress(
        on_progress,
        f"[atlas onboard] start Project={manifest.project_name} workspace={manifest.workspace_path}",
    )
    secrets = _load_secret_values(manifest)
    config = manifest.database.build_config(secrets)
    paths = _build_paths(manifest)
    for directory in (
        paths.workspace,
        paths.generated,
        paths.scans,
        paths.exports,
        paths.reports,
        paths.history,
        paths.diff,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    _emit_progress(
        on_progress,
        f"[atlas onboard] workspace Prepared output directories under {paths.generated}",
    )

    selection_plan = _build_selection_plan(manifest.ai)
    connection_reference, ai_reference, selection_json = _write_reference_files(
        manifest,
        include_ai=manifest.ai.enabled,
        selection_plan=selection_plan,
    )
    manifest.save()
    _emit_progress(
        on_progress,
        "[atlas onboard] manifest Saved local manifest and reference files.",
    )

    atlas = Atlas(config)
    _emit_progress(
        on_progress,
        f"[atlas onboard] scan Starting structural scan for {config.connection_string_safe}",
    )
    result = atlas.scan(
        on_progress=(
            lambda event: _emit_progress(on_progress, _render_scan_progress(event))
        ),
    )
    _emit_progress(
        on_progress,
        (
            "[atlas onboard] scan "
            f"Completed scan: {result.total_tables} tables, {result.total_views} views, "
            f"{result.total_columns} columns."
        ),
    )
    _emit_progress(
        on_progress,
        (
            "[atlas onboard] render "
            f"Rendering base sigilo style={manifest.sigilo_style} layout={manifest.sigilo_layout}."
        ),
    )
    sigilo = atlas.build_sigilo(
        result,
        style=cast(Literal["network", "seal", "compact"], manifest.sigilo_style),
        layout=cast(Literal["circular", "force"], manifest.sigilo_layout),
        schema_filter=config.schema_filter or None,
    )
    stem = sanitize_stem(result.database)
    scan_svg = paths.scans / f"{stem}.svg"
    scan_sigil = paths.scans / f"{stem}.sigil"
    scan_meta = paths.scans / f"{stem}_meta.json"
    scan_panel = paths.scans / f"{stem}_panel.html"
    scan_snapshot_path = paths.scans / f"{stem}.atlas"
    scan_svg.write_bytes(sigilo.svg_bytes)
    scan_sigil.write_text(
        json.dumps(result.to_dict(), ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    scan_meta.write_text(result.to_json(indent=2), encoding="utf-8")
    scan_panel.write_text(
        PanelBuilder(sigilo.svg_bytes, db_name=result.database).build_html(),
        encoding="utf-8",
    )
    base_snapshot = atlas.create_snapshot(result, sigilo)
    base_snapshot.save(scan_snapshot_path)
    _emit_progress(
        on_progress,
        f"[atlas onboard] save Wrote scan artifacts to {paths.scans}",
    )

    active_result = result
    active_svg = sigilo.svg_bytes
    active_snapshot = base_snapshot
    semantic_svg: Path | None = None
    semantic_sigil: Path | None = None
    semantic_meta: Path | None = None
    semantic_panel: Path | None = None
    semantic_snapshot_path: Path | None = None

    if manifest.ai.enabled:
        ai_config = manifest.ai.build_config(secrets)
        assert ai_config is not None
        (
            active_result,
            active_svg,
            active_snapshot,
            semantic_svg,
            semantic_sigil,
            semantic_meta,
            semantic_panel,
        ) = _run_semantic_enrichment(
            atlas,
            active_result,
            manifest,
            ai_config,
            paths,
            on_progress=on_progress,
        )
        semantic_snapshot_path = paths.semantic / f"{stem}_semantic.atlas"
        active_snapshot.save(semantic_snapshot_path)
        _emit_progress(
            on_progress,
            f"[atlas onboard] semantic snapshot Saved semantic snapshot to {semantic_snapshot_path}",
        )

    standalone_path = paths.exports / f"{stem}_standalone.html"
    _emit_progress(
        on_progress,
        "[atlas onboard] export Building standalone HTML export.",
    )
    StandaloneHTMLBuilder(
        active_svg.decode("utf-8"),
        db_name=active_result.database,
        has_semantics=bool(active_snapshot.semantics),
    ).export(standalone_path)

    health_report = paths.reports / f"{stem}_health_report.html"
    _emit_progress(
        on_progress,
        "[atlas onboard] report Generating health report.",
    )
    HTMLReportGenerator(active_result).generate(health_report, include_sigilo=True)
    executive_report = paths.reports / f"{stem}_executive_report.html"
    _emit_progress(
        on_progress,
        "[atlas onboard] report Generating executive report.",
    )
    ExecutiveReportGenerator(
        active_result,
        scores=active_snapshot.scores,
        anomalies=active_snapshot.anomalies,
        semantics=active_snapshot.semantics,
    ).export(executive_report)

    exporter = StructuredExporter(active_result, active_snapshot.semantics)
    _emit_progress(
        on_progress,
        "[atlas onboard] export Writing JSON, CSV, and Markdown dictionaries.",
    )
    dictionary_json = _write_text(paths.exports / "dictionary.json", exporter.export_json())
    tables_csv = _write_text(paths.exports / "tables.csv", exporter.export_csv_tables())
    columns_csv = _write_text(paths.exports / "columns.csv", exporter.export_csv_columns())
    dictionary_md = _write_text(paths.exports / "dictionary.md", exporter.export_markdown())

    history = AtlasHistory(paths.history)
    previous_snapshot_path = history.latest()
    diff_report: Path | None = None
    if previous_snapshot_path is not None:
        _emit_progress(
            on_progress,
            f"[atlas onboard] diff Comparing against previous snapshot {previous_snapshot_path.name}",
        )
        previous_snapshot = AtlasSnapshot.load(previous_snapshot_path)
        diff = SnapshotDiff.compare(previous_snapshot, active_snapshot)
        diff_report = paths.diff / f"{stem}_diff.html"
        SnapshotDiffReport().write(previous_snapshot, active_snapshot, diff, diff_report)
        _emit_progress(
            on_progress,
            f"[atlas onboard] diff Wrote diff report to {diff_report}",
        )
    else:
        _emit_progress(
            on_progress,
            "[atlas onboard] diff No previous snapshot found; skipping diff generation.",
        )

    history_snapshot_path = paths.history / history.build_snapshot_name(
        active_result.database,
        active_snapshot.manifest.created_at,
    )
    if history_snapshot_path.exists():
        stem = history_snapshot_path.stem
        suffix = history_snapshot_path.suffix
        counter = 2
        while history_snapshot_path.exists():
            history_snapshot_path = paths.history / f"{stem}_{counter}{suffix}"
            counter += 1
    active_snapshot.save(history_snapshot_path)
    _emit_progress(
        on_progress,
        f"[atlas onboard] history Saved current snapshot to {history_snapshot_path}",
    )
    _emit_progress(
        on_progress,
        f"[atlas onboard] done Full pipeline completed in {_format_elapsed(started_at)}",
    )

    return OnboardingOutputs(
        workspace=paths.workspace,
        manifest_path=manifest.manifest_path,
        env_path=manifest.env_file_path,
        scan_svg=scan_svg,
        scan_sigil=scan_sigil,
        scan_meta=scan_meta,
        scan_panel_html=scan_panel,
        scan_snapshot=scan_snapshot_path,
        standalone_html=standalone_path,
        health_report=health_report,
        executive_report=executive_report,
        dictionary_json=dictionary_json,
        tables_csv=tables_csv,
        columns_csv=columns_csv,
        dictionary_md=dictionary_md,
        history_snapshot=history_snapshot_path,
        diff_report=diff_report,
        semantic_svg=semantic_svg,
        semantic_sigil=semantic_sigil,
        semantic_meta=semantic_meta,
        semantic_panel_html=semantic_panel,
        semantic_snapshot=semantic_snapshot_path,
        connection_reference=connection_reference,
        ai_reference=ai_reference,
        selection_json=selection_json,
    )


__all__ = [
    "AISetup",
    "DatabaseSetup",
    "OnboardingManifest",
    "OnboardingOutputs",
    "SelectionPlan",
    "_build_selection_plan",
    "_ensure_local_ai_base_url",
    "_emit_progress",
    "_format_elapsed",
    "_parse_env_file",
    "_render_scan_progress",
    "_split_csv",
    "_write_env_file",
    "run_onboarding",
]
