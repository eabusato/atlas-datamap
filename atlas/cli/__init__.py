"""Atlas root CLI entrypoint."""

from __future__ import annotations

import sys
from typing import Any

import click

from atlas.version import ATLAS_C_SIGILO_VERSION, ATLAS_MIN_PYTHON, ATLAS_VERSION


def _check_python_version() -> None:
    required_major, required_minor = ATLAS_MIN_PYTHON
    if sys.version_info < (required_major, required_minor):
        click.echo(
            (
                "[atlas] Python "
                f"{required_major}.{required_minor}+ is required. "
                f"Current interpreter: {sys.version_info.major}.{sys.version_info.minor}."
            ),
            err=True,
        )
        raise SystemExit(1)


def _version_message() -> str:
    native_sigilo: Any | None
    try:
        import atlas._sigilo as native_sigilo
    except Exception:
        native_sigilo = None

    if native_sigilo is None or not native_sigilo.available():
        return f"atlas {ATLAS_VERSION} [native sigilo unavailable (Python fallback active)]"

    try:
        native_version = native_sigilo.render_version()
    except Exception:
        native_version = ATLAS_C_SIGILO_VERSION
    native_path = native_sigilo.library_path() or "unknown path"
    return f"atlas {ATLAS_VERSION} [native sigilo {native_version} ({native_path})]"


def _show_version(
    ctx: click.Context,
    param: click.Parameter,
    value: bool,
) -> None:
    del param
    if not value or ctx.resilient_parsing:
        return
    click.echo(_version_message())
    ctx.exit()


@click.group()
@click.option(
    "--version",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=_show_version,
    help="Show the Atlas version and native sigilo build information.",
)
def cli() -> None:
    """Atlas Datamap CLI root command."""
    _check_python_version()


def _register_subcommands() -> None:
    from atlas.cli.ask import ask_cmd
    from atlas.cli.diff import diff_cmd
    from atlas.cli.enrich import enrich_cmd
    from atlas.cli.export import export_group
    from atlas.cli.history import history_group
    from atlas.cli.info import info_cmd
    from atlas.cli.onboard import onboard_cmd
    from atlas.cli.open import open_cmd
    from atlas.cli.report import report_cmd
    from atlas.cli.scan import scan_cmd
    from atlas.cli.search import search_cmd

    cli.add_command(scan_cmd, name="scan")
    cli.add_command(open_cmd, name="open")
    cli.add_command(info_cmd, name="info")
    cli.add_command(search_cmd, name="search")
    cli.add_command(report_cmd, name="report")
    cli.add_command(onboard_cmd, name="onboard")
    cli.add_command(export_group, name="export")
    cli.add_command(enrich_cmd, name="enrich")
    cli.add_command(ask_cmd, name="ask")
    cli.add_command(diff_cmd, name="diff")
    cli.add_command(history_group, name="history")


_register_subcommands()
