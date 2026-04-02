"""Command implementation for ``atlas report``."""

from __future__ import annotations

from pathlib import Path

import click

from atlas.cli._common import resolve_config
from atlas.connectors import get_connector
from atlas.export.report import HTMLReportGenerator
from atlas.export.report_executive import ExecutiveReportGenerator
from atlas.export.snapshot import AtlasSnapshot
from atlas.introspection.runner import IntrospectionRunner
from atlas.types import IntrospectionResult


def _load_report_payload(
    *,
    db: str | None,
    config_path: str | None,
    sigil_path: Path | None,
    atlas_path: Path | None,
) -> tuple[
    IntrospectionResult,
    list[dict[str, object]] | None,
    list[dict[str, object]] | None,
    dict[str, object] | None,
]:
    if atlas_path is not None:
        snapshot = AtlasSnapshot.load(atlas_path)
        return (
            snapshot.result,
            snapshot.scores,
            snapshot.anomalies,
            snapshot.semantics,
        )
    if sigil_path is not None:
        return (
            IntrospectionResult.from_json(sigil_path.read_text(encoding="utf-8")),
            None,
            None,
            None,
        )
    config = resolve_config(db=db, config_path=config_path)
    connector = get_connector(config)
    return (IntrospectionRunner(config, connector).run(), None, None, None)


@click.command("report")
@click.option("--db", required=False, help="Database connection URL.")
@click.option("--config", "config_path", required=False, help="Path to atlas.toml.")
@click.option(
    "--sigil",
    "sigil_path",
    required=False,
    type=click.Path(exists=True, path_type=Path),
    help="Path to a pre-generated .sigil file.",
)
@click.option(
    "--atlas",
    "atlas_path",
    required=False,
    type=click.Path(exists=True, path_type=Path),
    help="Path to a pre-generated .atlas snapshot file.",
)
@click.option(
    "--output",
    "-o",
    default="report.html",
    show_default=True,
    type=click.Path(path_type=Path),
    help="Output HTML report path.",
)
@click.option(
    "--no-sigilo",
    "no_sigilo",
    is_flag=True,
    default=False,
    help="Skip the embedded sigilo section.",
)
@click.option(
    "--style",
    type=click.Choice(["health", "executive"]),
    default="health",
    show_default=True,
    help="Report style.",
)
@click.option("--quiet", is_flag=True, help="Suppress progress output.")
def report_cmd(
    db: str | None,
    config_path: str | None,
    sigil_path: Path | None,
    atlas_path: Path | None,
    output: Path,
    no_sigilo: bool,
    style: str,
    quiet: bool,
) -> None:
    """Generate a standalone HTML health report for an Atlas database snapshot."""

    live_requested = bool(db or config_path)
    source_count = int(live_requested) + int(sigil_path is not None) + int(atlas_path is not None)
    if source_count != 1:
        raise click.UsageError(
            "Provide either --db/--config or --sigil, or use --atlas alone."
        )

    try:
        if not quiet:
            if atlas_path is not None:
                click.echo(f"[atlas report] Loading snapshot from {atlas_path}", err=True)
            elif sigil_path is not None:
                click.echo(f"[atlas report] Loading sigil from {sigil_path}", err=True)
            else:
                click.echo("[atlas report] Running live introspection", err=True)
        result, scores, anomalies, semantics = _load_report_payload(
            db=db,
            config_path=config_path,
            sigil_path=sigil_path,
            atlas_path=atlas_path,
        )
        if not quiet:
            click.echo("[atlas report] Generating HTML report", err=True)
        if style == "health":
            HTMLReportGenerator(result).generate(output, include_sigilo=not no_sigilo)
        else:
            ExecutiveReportGenerator(
                result,
                scores=scores,
                anomalies=anomalies,
                semantics=semantics,
            ).export(output)
    except click.ClickException:
        raise
    except Exception as exc:
        raise click.ClickException(f"atlas report failed: {exc}") from exc

    output_path = output.resolve()
    if not quiet:
        click.echo(f"[atlas report] Report written to {output_path}", err=True)
    click.echo(str(output_path))
