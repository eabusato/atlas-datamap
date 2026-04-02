"""Command implementations for ``atlas history``."""

from __future__ import annotations

from pathlib import Path

import click

from atlas.cli.open import AtlasLocalServer
from atlas.export.diff import SnapshotDiff
from atlas.export.diff_report import SnapshotDiffReport
from atlas.export.snapshot import AtlasSnapshot
from atlas.history import AtlasHistory
from atlas.sigilo.panel import PanelBuilder


def _display_created_at(value: str) -> str:
    return value.replace("T", " ").replace("Z", "")[:19]


@click.group("history")
def history_group() -> None:
    """Inspect local Atlas snapshot history."""


@history_group.command("list")
@click.option(
    "--dir",
    "directory",
    type=click.Path(path_type=Path),
    default=Path("./snapshots"),
    show_default=True,
    help="Directory containing .atlas files.",
)
def history_list_cmd(directory: Path) -> None:
    """List snapshots in reverse chronological order."""

    history = AtlasHistory(directory)
    snapshots = history.list_snapshots()
    if not snapshots:
        click.echo(f"No valid snapshots found in {directory}.")
        return

    click.echo("Created At            | Database (Engine)         | File")
    click.echo("-" * 68)
    for path, manifest in snapshots:
        database_engine = f"{manifest.database} ({manifest.engine})"
        click.echo(f"{_display_created_at(manifest.created_at):<21} | {database_engine:<25} | {path.name}")


@history_group.command("diff")
@click.option(
    "--dir",
    "directory",
    type=click.Path(path_type=Path),
    default=Path("./snapshots"),
    show_default=True,
    help="Directory containing .atlas files.",
)
@click.option("--from", "from_ref", required=True, help="Older snapshot reference or 'latest'.")
@click.option("--to", "to_ref", required=True, help="Newer snapshot reference or 'latest'.")
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    required=True,
    help="Destination HTML report path.",
)
def history_diff_cmd(directory: Path, from_ref: str, to_ref: str, output: Path) -> None:
    """Resolve two snapshots from history and generate a diff report."""

    try:
        history = AtlasHistory(directory)
        before_path = history.resolve_snapshot(from_ref)
        after_path = history.resolve_snapshot(to_ref)
        before = AtlasSnapshot.load(before_path)
        after = AtlasSnapshot.load(after_path)
        diff = SnapshotDiff.compare(before, after)
        report_path = SnapshotDiffReport().write(before, after, diff, output)
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"[atlas history] Wrote diff report to {report_path}")


@history_group.command("open")
@click.option(
    "--dir",
    "directory",
    type=click.Path(path_type=Path),
    default=Path("./snapshots"),
    show_default=True,
    help="Directory containing .atlas files.",
)
@click.option("--date", "reference", required=True, help="Snapshot reference or 'latest'.")
@click.option("--port", default=8421, show_default=True, type=int, help="Local HTTP server port.")
def history_open_cmd(directory: Path, reference: str, port: int) -> None:
    """Open one historical snapshot in the local Atlas viewer."""

    try:
        history = AtlasHistory(directory)
        snapshot_path = history.resolve_snapshot(reference)
        snapshot = AtlasSnapshot.load(snapshot_path)
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    if not snapshot.sigil_svg.strip().startswith("<svg"):
        raise click.ClickException(f"Snapshot {snapshot_path.name} does not contain an inline SVG sigilo.")

    html = PanelBuilder(snapshot.sigil_svg.encode("utf-8"), db_name=snapshot.manifest.database).build_html()
    server = AtlasLocalServer(html, port=port)
    click.echo(f"[atlas history] Opening {snapshot_path.name}")
    server.start()
