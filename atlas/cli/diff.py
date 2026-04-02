"""Command implementation for ``atlas diff``."""

from __future__ import annotations

from pathlib import Path

import click

from atlas.export.diff import SnapshotDiff
from atlas.export.diff_report import SnapshotDiffReport
from atlas.export.snapshot import AtlasSnapshot


def _build_summary(diff: object) -> str:
    assert hasattr(diff, "added_tables")
    assert hasattr(diff, "removed_tables")
    assert hasattr(diff, "type_changes")
    assert hasattr(diff, "volume_changes")
    return (
        f"added_tables={len(diff.added_tables)} "
        f"removed_tables={len(diff.removed_tables)} "
        f"type_changes={len(diff.type_changes)} "
        f"volume_changes={len(diff.volume_changes)}"
    )


@click.command("diff")
@click.argument("before_file", type=click.Path(path_type=Path))
@click.argument("after_file", type=click.Path(path_type=Path))
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    required=True,
    help="Destination HTML report path.",
)
def diff_cmd(before_file: Path, after_file: Path, output: Path) -> None:
    """Compare two Atlas snapshots and write an offline HTML report."""

    try:
        before = AtlasSnapshot.load(before_file)
        after = AtlasSnapshot.load(after_file)
        diff = SnapshotDiff.compare(before, after)
        report_path = SnapshotDiffReport().write(before, after, diff, output)
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"[atlas diff] Wrote report to {report_path}")
    click.echo(f"[atlas diff] Summary: {_build_summary(diff)}")
