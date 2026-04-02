"""Command implementation for ``atlas scan``."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Literal, cast

import click

from atlas.cli._common import parse_csv_list, resolve_config
from atlas.config import PrivacyMode
from atlas.connectors import get_connector
from atlas.export.snapshot import artifact_paths, save_artifacts
from atlas.introspection.runner import IntrospectionError, IntrospectionRunner, _ProgressEvent
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.types import IntrospectionResult

_GENERIC_CONNECTOR_WARNING = (
    "[atlas scan] Using generic connector — structural metadata only "
    "(row counts, sizes, and advanced statistics are unavailable)."
)


def _artifact_stem(database: str) -> str | None:
    if not database:
        return None
    raw = Path(database)
    name = raw.name
    return name or database


def _render_progress(event: _ProgressEvent, *, quiet: bool) -> None:
    if quiet:
        return
    prefix = f"[atlas scan] {event.stage}"
    if event.total > 0 and event.current > 0:
        prefix += f" {event.current}/{event.total}"
    prefix += f" [{event.elapsed_ms} ms]"
    click.echo(f"{prefix} {event.message}", err=True)


def _emit_stage(message: str, *, stage: str, quiet: bool, started_at: float) -> None:
    if quiet:
        return
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    click.echo(f"[atlas scan] {stage} [{elapsed_ms} ms] {message}", err=True)


def _check_overwrite(paths: tuple[Path, Path, Path], *, force: bool) -> None:
    existing = [path for path in paths if path.exists()]
    if existing and not force:
        joined = ", ".join(str(path) for path in existing)
        raise click.ClickException(
            "Refusing to overwrite existing artifacts without --force: " + joined
        )


def _build_svg(
    result: IntrospectionResult,
    *,
    style: Literal["network", "seal", "compact"],
    layout: Literal["circular", "force"],
    schema_filter: list[str],
) -> bytes:
    builder = DatamapSigiloBuilder.from_introspection_result(result)
    if schema_filter:
        builder.set_schema_filter(schema_filter)
    builder.set_style(style)
    builder.set_layout(layout)
    return builder.build()


@click.command("scan")
@click.option("--db", required=False, help="Database connection URL.")
@click.option("--config", "config_path", required=False, help="Path to atlas.toml.")
@click.option("--schema", required=False, help="Comma-separated schema filter.")
@click.option(
    "--output",
    default="./out",
    show_default=True,
    type=click.Path(path_type=Path),
    help="Output directory.",
)
@click.option(
    "--style",
    default="network",
    show_default=True,
    type=click.Choice(["network", "seal", "compact"], case_sensitive=False),
    help="Visual style preset.",
)
@click.option(
    "--layout",
    default="circular",
    show_default=True,
    type=click.Choice(["circular", "force"], case_sensitive=False),
    help="Node layout algorithm.",
)
@click.option(
    "--privacy",
    required=False,
    type=click.Choice([mode.value for mode in PrivacyMode], case_sensitive=False),
    help="Privacy mode override.",
)
@click.option("--dry-run", is_flag=True, help="Inspect and render without writing artifacts.")
@click.option("--force", is_flag=True, help="Overwrite existing artifacts.")
@click.option("--quiet", is_flag=True, help="Suppress incremental progress output.")
def scan_cmd(
    db: str | None,
    config_path: str | None,
    schema: str | None,
    output: Path,
    style: str,
    layout: str,
    privacy: str | None,
    dry_run: bool,
    force: bool,
    quiet: bool,
) -> None:
    """Scan a database and generate a navigable data map."""

    started_at = time.perf_counter()
    schema_filter = parse_csv_list(schema)
    config = resolve_config(db=db, config_path=config_path, schema_csv=schema, privacy=privacy)
    connector = get_connector(config)
    if config.engine.value == "generic":
        click.echo(_GENERIC_CONNECTOR_WARNING, err=True)
    runner = IntrospectionRunner(
        config,
        connector,
        on_progress=lambda event: _render_progress(event, quiet=quiet),
    )

    try:
        result = runner.run()
    except IntrospectionError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:
        raise click.ClickException(f"atlas scan failed: {exc}") from exc

    if dry_run:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        click.echo(
            f"Dry run completed for {result.database}: "
            f"{result.total_tables} tables, {result.total_views} views, "
            f"{result.total_columns} columns in {elapsed_ms} ms."
        )
        return

    _emit_stage("Rendering SVG datamap", stage="render", quiet=quiet, started_at=started_at)
    try:
        svg_bytes = _build_svg(
            result,
            style=cast(Literal["network", "seal", "compact"], style.lower()),
            layout=cast(Literal["circular", "force"], layout.lower()),
            schema_filter=schema_filter,
        )
    except Exception as exc:
        raise click.ClickException(f"Failed to render sigilo: {exc}") from exc

    stem = _artifact_stem(result.database)
    planned = artifact_paths(result, output, stem=stem)
    _check_overwrite(
        (planned.svg_path, planned.meta_json_path, planned.sigil_path),
        force=force,
    )
    _emit_stage("Writing artifacts", stage="save", quiet=quiet, started_at=started_at)
    artifacts = save_artifacts(result, svg_bytes, output, stem=stem)

    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    click.echo(
        f"Scan completed for {result.database}: "
        f"{result.total_tables} tables, {result.total_views} views, "
        f"{result.total_columns} columns in {elapsed_ms} ms."
    )
    click.echo(f"SVG: {artifacts.svg_path}")
    click.echo(f"SIGIL: {artifacts.sigil_path}")
    click.echo(f"META: {artifacts.meta_json_path}")
