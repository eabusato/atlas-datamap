"""Command implementation for ``atlas search``."""

from __future__ import annotations

import click

from atlas.analysis.classifier import TableClassifier
from atlas.cli._common import resolve_config
from atlas.connectors import get_connector
from atlas.introspection.runner import IntrospectionError, IntrospectionRunner
from atlas.search import AtlasSearch
from atlas.search.types import SearchResult


def _render_result(result: SearchResult) -> str:
    score = f"{result.score:.1f}".rstrip("0").rstrip(".")
    return f"[{result.entity_type.value}] {result.qualified_name} score={score} reason={result.reason}"


def _load_search_index(db: str | None, config_path: str | None) -> AtlasSearch:
    config = resolve_config(db=db, config_path=config_path)
    connector = get_connector(config)
    runner = IntrospectionRunner(config, connector)
    try:
        result = runner.run()
    except IntrospectionError:
        raise
    except Exception as exc:  # pragma: no cover - click exception wrapping
        raise IntrospectionError(str(exc)) from exc
    TableClassifier().classify_all(result)
    return AtlasSearch(result)


@click.command("search")
@click.argument("query")
@click.option("--db", required=False, help="Database connection URL.")
@click.option("--config", "config_path", required=False, help="Path to atlas.toml.")
@click.option("--schema", "schema_filter", required=False, help="Restrict search to one schema.")
@click.option("--type", "type_filter", required=False, help="Restrict table search to one heuristic type.")
@click.option(
    "--columns",
    "columns_only",
    is_flag=True,
    help="Search columns only instead of mixed schema/table discovery.",
)
def search_cmd(
    query: str,
    db: str | None,
    config_path: str | None,
    schema_filter: str | None,
    type_filter: str | None,
    columns_only: bool,
) -> None:
    """Search Atlas metadata by schema, table, or column text."""

    if not query.strip():
        raise click.UsageError("Provide a non-empty query string.")
    if columns_only and type_filter is not None:
        raise click.UsageError("--type cannot be combined with --columns.")

    try:
        search = _load_search_index(db, config_path)
    except IntrospectionError as exc:
        raise click.ClickException(f"atlas search failed: {exc}") from exc

    if columns_only:
        results = search.search_columns(query, schema_filter=schema_filter)
    elif schema_filter is not None or type_filter is not None:
        results = search.search_tables(
            query,
            schema_filter=schema_filter,
            type_filter=type_filter,
        )
    else:
        results = search.search_schema(query)

    if not results:
        click.echo(f"No matches found for query {query!r}.")
        return

    for result in results:
        click.echo(_render_result(result))
