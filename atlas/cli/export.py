"""Command implementations for ``atlas export``."""

from __future__ import annotations

from pathlib import Path

import click

from atlas.export.snapshot import AtlasSnapshot
from atlas.export.standalone import StandaloneHTMLBuilder
from atlas.export.structured import StructuredExporter
from atlas.sigilo.builder import SigiloBuilder
from atlas.types import IntrospectionResult


class ExportSource:
    """Resolved export input source."""

    def __init__(
        self,
        *,
        result: IntrospectionResult,
        semantics: dict[str, object] | None = None,
        svg_content: str | None = None,
        database_name: str,
    ) -> None:
        self.result = result
        self.semantics = semantics
        self.svg_content = svg_content
        self.database_name = database_name


def _validate_input_source(sigil_path: Path | None, atlas_path: Path | None) -> None:
    source_count = int(sigil_path is not None) + int(atlas_path is not None)
    if source_count != 1:
        raise click.UsageError("Provide exactly one source: --sigil or --atlas.")


def load_export_source(
    *,
    sigil_path: Path | None,
    atlas_path: Path | None,
) -> ExportSource:
    """Resolve one export source into a shared runtime object."""

    _validate_input_source(sigil_path, atlas_path)
    if sigil_path is not None:
        result = IntrospectionResult.from_json(sigil_path.read_text(encoding="utf-8"))
        svg_content = SigiloBuilder(result).build_svg().decode("utf-8")
        return ExportSource(
            result=result,
            semantics=None,
            svg_content=svg_content,
            database_name=result.database,
        )

    assert atlas_path is not None
    snapshot = AtlasSnapshot.load(atlas_path)
    return ExportSource(
        result=snapshot.result,
        semantics=snapshot.semantics,
        svg_content=snapshot.sigil_svg,
        database_name=snapshot.manifest.database,
    )


@click.group("export")
def export_group() -> None:
    """Export Atlas outputs into offline and structured formats."""


@export_group.command("svg")
@click.option("--sigil", "sigil_path", type=click.Path(exists=True, path_type=Path))
@click.option("--atlas", "atlas_path", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", type=click.Path(path_type=Path), required=True)
@click.option(
    "--include-semantics/--no-semantics",
    default=None,
    help="Include semantic descriptions in the standalone HTML. Default: auto-detect from source.",
)
def export_svg_cmd(
    sigil_path: Path | None,
    atlas_path: Path | None,
    output: Path,
    include_semantics: bool | None,
) -> None:
    """Export an offline standalone HTML wrapper for one Atlas sigilo."""

    try:
        source = load_export_source(sigil_path=sigil_path, atlas_path=atlas_path)
        assert source.svg_content is not None
        use_semantics = bool(source.semantics) if include_semantics is None else include_semantics
        target = StandaloneHTMLBuilder(
            source.svg_content,
            db_name=source.database_name,
            has_semantics=bool(source.semantics),
            include_semantics=use_semantics,
        ).export(output)
    except click.ClickException:
        raise
    except Exception as exc:
        raise click.ClickException(f"atlas export svg failed: {exc}") from exc
    click.echo(str(target.resolve()))


@export_group.command("json")
@click.option("--sigil", "sigil_path", type=click.Path(exists=True, path_type=Path))
@click.option("--atlas", "atlas_path", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", type=click.Path(path_type=Path), required=True)
def export_json_cmd(
    sigil_path: Path | None,
    atlas_path: Path | None,
    output: Path,
) -> None:
    """Export Atlas metadata as JSON."""

    try:
        source = load_export_source(sigil_path=sigil_path, atlas_path=atlas_path)
        payload = StructuredExporter(source.result, semantics=source.semantics).export_json()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload, encoding="utf-8")
    except click.ClickException:
        raise
    except Exception as exc:
        raise click.ClickException(f"atlas export json failed: {exc}") from exc
    click.echo(str(output.resolve()))


@export_group.command("csv")
@click.option("--sigil", "sigil_path", type=click.Path(exists=True, path_type=Path))
@click.option("--atlas", "atlas_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--entity",
    type=click.Choice(["tables", "columns"]),
    required=True,
    help="CSV export target.",
)
@click.option("--output", "-o", type=click.Path(path_type=Path), required=True)
def export_csv_cmd(
    sigil_path: Path | None,
    atlas_path: Path | None,
    entity: str,
    output: Path,
) -> None:
    """Export Atlas metadata as CSV."""

    try:
        source = load_export_source(sigil_path=sigil_path, atlas_path=atlas_path)
        exporter = StructuredExporter(source.result, semantics=source.semantics)
        payload = (
            exporter.export_csv_tables()
            if entity == "tables"
            else exporter.export_csv_columns()
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload, encoding="utf-8", newline="")
    except click.ClickException:
        raise
    except Exception as exc:
        raise click.ClickException(f"atlas export csv failed: {exc}") from exc
    click.echo(str(output.resolve()))


@export_group.command("markdown")
@click.option("--sigil", "sigil_path", type=click.Path(exists=True, path_type=Path))
@click.option("--atlas", "atlas_path", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", type=click.Path(path_type=Path), required=True)
def export_markdown_cmd(
    sigil_path: Path | None,
    atlas_path: Path | None,
    output: Path,
) -> None:
    """Export Atlas metadata as Markdown."""

    try:
        source = load_export_source(sigil_path=sigil_path, atlas_path=atlas_path)
        payload = StructuredExporter(source.result, semantics=source.semantics).export_markdown()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload, encoding="utf-8")
    except click.ClickException:
        raise
    except Exception as exc:
        raise click.ClickException(f"atlas export markdown failed: {exc}") from exc
    click.echo(str(output.resolve()))
