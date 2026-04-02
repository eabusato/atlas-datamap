"""Command implementation for ``atlas info``."""

from __future__ import annotations

from dataclasses import replace

import click

from atlas.cli._common import resolve_config
from atlas.cli._info_format import render_json, render_text, render_yaml
from atlas.connectors import get_connector
from atlas.connectors.base import BaseConnector
from atlas.types import TableInfo


class TableNotFoundError(LookupError):
    """Raised when a requested table is not present in a schema."""

    def __init__(self, schema: str, table: str) -> None:
        super().__init__(f"Table '{schema}.{table}' not found.")
        self.schema = schema
        self.table = table


def _parse_table_ref(table_ref: str) -> tuple[str, str]:
    """Split a table reference into ``(schema, table_name)``."""

    reference = table_ref.strip()
    if not reference:
        raise click.UsageError("Provide a non-empty --table value.")

    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    for char in reference:
        if char == '"':
            in_quotes = not in_quotes
            continue
        if char == "." and not in_quotes:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    parts.append("".join(current).strip())

    if len(parts) == 1:
        schema, table_name = "public", parts[0]
    elif len(parts) == 2:
        schema, table_name = parts
    else:
        raise click.UsageError(
            f"Invalid --table value {table_ref!r}. Expected 'schema.table' or 'table'."
        )
    if not schema or not table_name:
        raise click.UsageError(
            f"Invalid --table value {table_ref!r}. Expected 'schema.table' or 'table'."
        )
    return schema, table_name


def _fetch_table_info(
    connector: BaseConnector,
    schema: str,
    table_name: str,
    *,
    include_columns: bool = True,
    include_fks: bool = True,
    include_indexes: bool = True,
) -> TableInfo:
    """Load metadata selectively for the requested table only."""

    tables = connector.get_tables(schema)
    stub = next((table for table in tables if table.name == table_name), None)
    if stub is None:
        raise TableNotFoundError(schema, table_name)

    table_info = replace(stub)
    table_info.columns = connector.get_columns(schema, table_name) if include_columns else []
    table_info.foreign_keys = connector.get_foreign_keys(schema, table_name) if include_fks else []
    table_info.indexes = connector.get_indexes(schema, table_name) if include_indexes else []
    table_info.row_count_estimate = connector.get_row_count_estimate(schema, table_name)
    table_info.size_bytes = connector.get_table_size_bytes(schema, table_name)
    table_info.column_count = len(table_info.columns) if table_info.columns else stub.column_count

    indexed_columns = {column for index in table_info.indexes for column in index.columns}
    fk_source_columns = {
        column
        for foreign_key in table_info.foreign_keys
        for column in foreign_key.source_columns
    }
    for column in table_info.columns:
        if column.name in indexed_columns:
            column.is_indexed = True
        if column.name in fk_source_columns:
            column.is_foreign_key = True
    table_info.refresh_derived_fields()
    return table_info


@click.command("info")
@click.option("--db", required=False, help="Database connection URL.")
@click.option("--config", "config_path", required=False, help="Path to atlas.toml.")
@click.option("--table", "table_ref", required=True, help="Qualified table name.")
@click.option(
    "--format",
    "output_format",
    default="text",
    show_default=True,
    type=click.Choice(["text", "json", "yaml"], case_sensitive=False),
    help="Output format.",
)
@click.option("--columns/--no-columns", default=True, show_default=True, help="Include column details.")
@click.option("--indexes/--no-indexes", default=True, show_default=True, help="Include index details.")
@click.option("--fks/--no-fks", default=True, show_default=True, help="Include foreign key details.")
def info_cmd(
    db: str | None,
    config_path: str | None,
    table_ref: str,
    output_format: str,
    columns: bool,
    indexes: bool,
    fks: bool,
) -> None:
    """Display metadata for a specific table."""

    config = resolve_config(db=db, config_path=config_path)
    schema, table_name = _parse_table_ref(table_ref)
    connector = get_connector(config)
    try:
        connector.connect()
        table_info = _fetch_table_info(
            connector,
            schema,
            table_name,
            include_columns=columns,
            include_fks=fks,
            include_indexes=indexes,
        )
    except TableNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:
        raise click.ClickException(f"atlas info failed: {exc}") from exc
    finally:
        connector.disconnect()

    formatter = {
        "text": render_text,
        "json": render_json,
        "yaml": render_yaml,
    }[output_format.lower()]
    click.echo(
        formatter(
            table_info,
            include_columns=columns,
            include_fks=fks,
            include_indexes=indexes,
        )
    )
