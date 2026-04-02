"""Command implementation for ``atlas enrich``."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import click

from atlas.ai import AIConfig, AIConfigError, SemanticCache, SemanticEnricher, build_client
from atlas.cli._common import require_existing_path, resolve_config
from atlas.config import AtlasConnectionConfig, PrivacyMode
from atlas.connectors import get_connector
from atlas.export.snapshot import save_artifacts
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.types import IntrospectionResult


@dataclass(slots=True)
class _SelectionPlan:
    schema_names: set[str] | None
    tables_by_schema: dict[str, set[str]]
    columns_by_table: dict[tuple[str, str], set[str] | None]


def _artifact_base_name(result: IntrospectionResult, sigil_path: Path | None) -> str:
    if sigil_path is not None:
        return f"{sigil_path.stem}_semantic"
    database_name = Path(result.database).name if result.database else "atlas"
    return f"{database_name or 'atlas'}_semantic"


def _load_sigil_result(sigil_path: Path) -> IntrospectionResult:
    payload = sigil_path.read_text(encoding="utf-8")
    return IntrospectionResult.from_json(payload)


def _clone_filtered_result(
    result: IntrospectionResult,
    *,
    selection: _SelectionPlan | None = None,
) -> IntrospectionResult:
    filtered = IntrospectionResult.from_dict(result.to_dict())
    if selection is not None:
        if selection.schema_names is not None:
            filtered.schemas = [
                schema for schema in filtered.schemas if schema.name in selection.schema_names
            ]
        for schema in filtered.schemas:
            selected_tables = selection.tables_by_schema.get(schema.name)
            if selected_tables is not None:
                schema.tables = [
                    table for table in schema.tables if table.name in selected_tables
                ]
            schema.refresh_derived_fields()
        filtered.schemas = [schema for schema in filtered.schemas if schema.tables]
    filtered._compute_summary()
    filtered._apply_fk_in_degree()
    if not filtered.schemas:
        raise click.ClickException("No objects matched the requested enrichment scope.")
    return filtered


def _load_selection_plan(
    selection_path: Path | None,
    *,
    schema: str | None,
    table: str | None,
) -> _SelectionPlan | None:
    schema_names: set[str] | None = None
    tables_by_schema: dict[str, set[str]] = {}
    columns_by_table: dict[tuple[str, str], set[str] | None] = {}

    if selection_path is not None:
        try:
            payload = json.loads(selection_path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise click.ClickException(f"Selection file not found: {selection_path}") from exc
        except json.JSONDecodeError as exc:
            raise click.ClickException(f"Failed to parse JSON from {selection_path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise click.ClickException("Selection JSON must be an object.")

        raw_schemas = payload.get("schemas")
        if raw_schemas is not None:
            if not isinstance(raw_schemas, list) or not all(isinstance(item, str) for item in raw_schemas):
                raise click.ClickException("'schemas' must be a list of strings.")
            schema_names = {item.strip() for item in raw_schemas if item.strip()} or None

        raw_tables = payload.get("tables", {})
        if raw_tables is not None:
            if not isinstance(raw_tables, dict):
                raise click.ClickException("'tables' must be an object keyed by schema.")
            for schema_name, raw_table_names in raw_tables.items():
                if not isinstance(schema_name, str):
                    raise click.ClickException("Schema names in 'tables' must be strings.")
                if not isinstance(raw_table_names, list) or not all(
                    isinstance(item, str) for item in raw_table_names
                ):
                    raise click.ClickException(
                        f"'tables.{schema_name}' must be a list of table names."
                    )
                normalized_schema = schema_name.strip()
                normalized_tables = {item.strip() for item in raw_table_names if item.strip()}
                tables_by_schema[normalized_schema] = normalized_tables

        raw_columns = payload.get("columns", {})
        if raw_columns is not None:
            if not isinstance(raw_columns, dict):
                raise click.ClickException("'columns' must be an object keyed by 'schema.table'.")
            for table_ref, raw_column_names in raw_columns.items():
                if not isinstance(table_ref, str) or "." not in table_ref:
                    raise click.ClickException(
                        "Column selectors must use the 'schema.table' key format."
                    )
                if not isinstance(raw_column_names, list) or not all(
                    isinstance(item, str) for item in raw_column_names
                ):
                    raise click.ClickException(
                        f"'columns.{table_ref}' must be a list of column names."
                    )
                schema_name, table_name = table_ref.split(".", 1)
                normalized_schema = schema_name.strip()
                normalized_table = table_name.strip()
                normalized_columns = {item.strip() for item in raw_column_names if item.strip()}
                columns_by_table[(normalized_schema, normalized_table)] = normalized_columns
                tables_by_schema.setdefault(normalized_schema, set()).add(normalized_table)

        if schema_names is None and tables_by_schema:
            schema_names = set(tables_by_schema)
        elif schema_names is not None:
            schema_names.update(tables_by_schema)

    if schema is not None:
        schema_names = {schema} if schema_names is None else schema_names & {schema}
    if table is not None:
        assert schema is not None
        existing_tables = tables_by_schema.get(schema)
        tables_by_schema[schema] = {table} if existing_tables is None else existing_tables & {table}
        if schema_names is None:
            schema_names = {schema}
        else:
            schema_names &= {schema}

    if schema_names is None and not tables_by_schema and not columns_by_table:
        return None
    return _SelectionPlan(
        schema_names=schema_names,
        tables_by_schema=tables_by_schema,
        columns_by_table=columns_by_table,
    )


def _selection_schema_csv(selection: _SelectionPlan | None) -> str | None:
    if selection is None or selection.schema_names is None:
        return None
    return ",".join(sorted(selection.schema_names))


def _selected_column_count(
    result: IntrospectionResult,
    selection: _SelectionPlan | None,
) -> int:
    total = 0
    for table_info in result.all_tables():
        if selection is None:
            total += len(table_info.columns)
            continue
        selected = selection.columns_by_table.get((table_info.schema, table_info.name))
        if selected is None:
            total += len(table_info.columns)
        else:
            total += sum(1 for column in table_info.columns if column.name in selected)
    return total


def _render_semantic_svg(result: IntrospectionResult) -> bytes:
    return DatamapSigiloBuilder.from_introspection_result(result).rebuild_with_semantics()


def _emit_table_progress(
    progress: Any,
    finished_table_name: str,
    column_count: int,
    current: int,
    total: int,
) -> None:
    progress.update(1)
    click.echo(
        f"[atlas enrich] table {current}/{total}: {finished_table_name} ({column_count} columns)"
    )


def _emit_column_progress(
    table_name: str,
    column_name: str,
    column_index: int,
    column_total: int,
    table_index: int,
    table_total: int,
) -> None:
    click.echo(
        f"[atlas enrich] table {table_index}/{table_total}: "
        f"{table_name} | column {column_index}/{column_total}: {column_name}"
    )


@click.command("enrich")
@click.option("--sigil", "sigil_path", type=click.Path(path_type=Path), required=False)
@click.option("--db", type=str, required=False, help="Database connection URL.")
@click.option("--config", "config_path", type=click.Path(path_type=Path), required=False)
@click.option(
    "--ai-config",
    "ai_config_path",
    type=click.Path(path_type=Path),
    required=False,
    help="TOML file with the [ai] section for the local model backend.",
)
@click.option("--schema", type=str, required=False, help="Restrict enrichment to one schema.")
@click.option("--table", type=str, required=False, help="Restrict enrichment to one table.")
@click.option(
    "--selection",
    "selection_path",
    type=click.Path(path_type=Path),
    required=False,
    help="JSON file describing schemas/tables/columns to enrich.",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    default=Path("./out"),
    show_default=True,
    help="Output directory for enriched artifacts.",
)
@click.option(
    "--parallel",
    type=int,
    default=2,
    show_default=True,
    help="Number of concurrent table workers.",
)
@click.option("--force", is_flag=True, help="Ignore semantic cache and re-run the LLM.")
@click.option(
    "--tables-only",
    is_flag=True,
    help="Compatibility alias for --column-mode skip.",
)
@click.option(
    "--column-mode",
    type=click.Choice(["infer", "full", "skip"], case_sensitive=False),
    default="infer",
    show_default=True,
    help="Choose whether columns are inferred, fully enriched with the LLM, or skipped.",
)
@click.option("--dry-run", is_flag=True, help="Show the planned workload without calling the LLM.")
def enrich_cmd(
    sigil_path: Path | None,
    db: str | None,
    config_path: Path | None,
    ai_config_path: Path | None,
    schema: str | None,
    table: str | None,
    selection_path: Path | None,
    output: Path,
    parallel: int,
    force: bool,
    tables_only: bool,
    column_mode: str,
    dry_run: bool,
) -> None:
    """Enrich an Atlas sigilo with semantic descriptions from a local LLM."""

    input_count = sum(value is not None for value in (sigil_path, db, config_path))
    if input_count != 1:
        raise click.UsageError(
            "Provide exactly one structural source: --sigil, --db, or --config."
        )
    if table is not None and schema is None:
        raise click.UsageError("--table requires --schema.")
    if parallel < 1:
        raise click.UsageError("--parallel must be >= 1.")

    effective_column_mode: Literal["infer", "full", "skip"]
    effective_column_mode = cast(
        Literal["infer", "full", "skip"],
        "skip" if tables_only else column_mode.lower(),
    )

    result: IntrospectionResult
    connector = None
    connection_config: AtlasConnectionConfig | None = None
    resolved_sigil_path = require_existing_path(sigil_path, kind="file") if sigil_path else None
    resolved_config_path = require_existing_path(config_path, kind="file") if config_path else None
    resolved_ai_config_path = (
        require_existing_path(ai_config_path, kind="file") if ai_config_path else None
    )
    resolved_selection_path = (
        require_existing_path(selection_path, kind="file") if selection_path else None
    )
    selection = _load_selection_plan(resolved_selection_path, schema=schema, table=table)

    if not dry_run:
        try:
            ai_config = (
                AIConfig.from_file(resolved_ai_config_path)
                if resolved_ai_config_path
                else AIConfig()
            )
            client = build_client(ai_config)
        except AIConfigError as exc:
            raise click.ClickException(str(exc)) from exc
        except Exception as exc:
            raise click.ClickException(f"Failed to initialize local AI client: {exc}") from exc

        click.echo("[atlas enrich] Checking local AI availability...")
        if not client.is_available():
            raise click.ClickException(
                "Local AI provider is unavailable. Start the configured local server and retry."
            )
        model_info = client.get_model_info()
        click.echo(
            f"[atlas enrich] Using local model {model_info.model_name} via {model_info.provider_name}."
        )
        cache = SemanticCache(output)
        enricher = SemanticEnricher(client, cache=cache)
    else:
        enricher = None

    if resolved_sigil_path is not None:
        click.echo(f"[atlas enrich] Loading introspection from {resolved_sigil_path.name}...")
        result = _load_sigil_result(resolved_sigil_path)
        result = _clone_filtered_result(result, selection=selection)
        if dry_run:
            selected_columns = _selected_column_count(result, selection)
            click.echo(
                "[atlas enrich] Dry run: "
                f"{len(result.schemas)} schema(s), {result.total_tables} table(s), "
                f"{selected_columns} selected column(s). "
                f"Column mode={effective_column_mode}."
            )
            return
    else:
        try:
            connection_config = resolve_config(
                db=db,
                config_path=str(resolved_config_path) if resolved_config_path else None,
                schema_csv=_selection_schema_csv(selection),
            )
            connector = get_connector(connection_config)
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc

        click.echo(
            f"[atlas enrich] Connecting to {connection_config.connection_string_safe}..."
        )
        with connector.session():
            result = connector.introspect_all()
            result = _clone_filtered_result(result, selection=selection)
            if dry_run:
                selected_columns = _selected_column_count(result, selection)
                click.echo(
                    "[atlas enrich] Dry run: "
                    f"{len(result.schemas)} schema(s), {result.total_tables} table(s), "
                    f"{selected_columns} selected column(s). "
                    f"Column mode={effective_column_mode}."
                )
                return
            assert enricher is not None
            for schema_info in result.schemas:
                click.echo(
                    f"[atlas enrich] Enriching schema {schema_info.name} ({len(schema_info.tables)} tables)..."
                )
                with click.progressbar(
                    length=len(schema_info.tables),
                    label=f"[atlas enrich] {schema_info.name}",
                    show_pos=True,
                ) as progress:
                    def _on_table_complete(
                        finished_table: Any,
                        current: int,
                        total: int,
                        *,
                        progress_bar: Any = progress,
                    ) -> None:
                        _emit_table_progress(
                            progress_bar,
                            finished_table.qualified_name,
                            len(finished_table.columns),
                            current,
                            total,
                        )

                    enricher.enrich_schema(
                        schema_info,
                        connector,
                        connection_config.privacy_mode,
                        parallel_workers=parallel,
                        force_recompute=force,
                        tables_only=tables_only,
                        column_mode=effective_column_mode,
                        selected_columns_by_table=selection.columns_by_table if selection else None,
                        on_table_complete=_on_table_complete,
                        on_column_complete=lambda finished_table, column, column_index, column_total, table_index, table_total: _emit_column_progress(
                            finished_table.qualified_name,
                            column.name,
                            column_index,
                            column_total,
                            table_index,
                            table_total,
                        ),
                    )
        assert connector is not None
        output.mkdir(parents=True, exist_ok=True)
        svg_bytes = _render_semantic_svg(result)
        stem = _artifact_base_name(result, resolved_sigil_path)
        artifacts = save_artifacts(result, svg_bytes, output, stem=stem)
        click.echo(f"[atlas enrich] Writing enriched artifacts to {output}")
        click.echo(f"SVG: {artifacts.svg_path}")
        click.echo(f"SIGIL: {artifacts.sigil_path}")
        click.echo(f"META: {artifacts.meta_json_path}")
        return

    assert enricher is not None
    for schema_info in result.schemas:
        click.echo(
            f"[atlas enrich] Enriching schema {schema_info.name} ({len(schema_info.tables)} tables)..."
        )
        with click.progressbar(
            length=len(schema_info.tables),
            label=f"[atlas enrich] {schema_info.name}",
            show_pos=True,
        ) as progress:
            def _on_table_complete(
                finished_table: Any,
                current: int,
                total: int,
                *,
                progress_bar: Any = progress,
            ) -> None:
                _emit_table_progress(
                    progress_bar,
                    finished_table.qualified_name,
                    len(finished_table.columns),
                    current,
                    total,
                )

            enricher.enrich_schema(
                schema_info,
                None,
                PrivacyMode.no_samples,
                parallel_workers=parallel,
                force_recompute=force,
                tables_only=tables_only,
                column_mode=effective_column_mode,
                selected_columns_by_table=selection.columns_by_table if selection else None,
                on_table_complete=_on_table_complete,
                on_column_complete=lambda finished_table, column, column_index, column_total, table_index, table_total: _emit_column_progress(
                    finished_table.qualified_name,
                    column.name,
                    column_index,
                    column_total,
                    table_index,
                    table_total,
                ),
            )

    output.mkdir(parents=True, exist_ok=True)
    svg_bytes = _render_semantic_svg(result)
    stem = _artifact_base_name(result, resolved_sigil_path)
    artifacts = save_artifacts(result, svg_bytes, output, stem=stem)
    click.echo(f"[atlas enrich] Writing enriched artifacts to {output}")
    click.echo(f"SVG: {artifacts.svg_path}")
    click.echo(f"SIGIL: {artifacts.sigil_path}")
    click.echo(f"META: {artifacts.meta_json_path}")
