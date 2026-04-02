"""Interactive onboarding flow for full Atlas runs."""

from __future__ import annotations

import json
from pathlib import Path

import click

from atlas.cli._common import require_existing_path
from atlas.config import DatabaseEngine, PrivacyMode
from atlas.onboarding import (
    AISetup,
    DatabaseSetup,
    OnboardingManifest,
    _ensure_local_ai_base_url,
    _split_csv,
    _write_env_file,
    run_onboarding,
)


def _engine_prompt() -> str:
    return str(
        click.prompt(
            "Database engine",
            type=click.Choice([engine.value for engine in DatabaseEngine], case_sensitive=False),
            default=DatabaseEngine.sqlite.value,
            show_choices=True,
        )
    ).lower()


def _privacy_prompt() -> str:
    return str(
        click.prompt(
            "Privacy mode",
            type=click.Choice([mode.value for mode in PrivacyMode], case_sensitive=False),
            default=PrivacyMode.masked.value,
            show_choices=True,
        )
    ).lower()


def _ai_provider_prompt() -> str:
    return str(
        click.prompt(
            "Local AI provider",
            type=click.Choice(["ollama", "llamacpp", "openai_compatible"], case_sensitive=False),
            default="ollama",
            show_choices=True,
        )
    ).lower()


def _column_mode_prompt() -> str:
    return str(
        click.prompt(
            "Column analysis mode",
            type=click.Choice(["infer", "full", "skip"], case_sensitive=False),
            default="infer",
            show_choices=True,
        )
    ).lower()


def _collect_database_setup(*, managed_env: bool) -> tuple[DatabaseSetup, dict[str, str]]:
    engine = _engine_prompt()
    env_values: dict[str, str] = {}

    if engine == DatabaseEngine.sqlite.value:
        sqlite_path = click.prompt(
            "SQLite database path",
            type=str,
            default="./database.sqlite",
        )
        setup = DatabaseSetup(
            engine=engine,
            sqlite_path=sqlite_path,
            database=sqlite_path,
        )
    elif engine == DatabaseEngine.generic.value:
        url_env_var = click.prompt(
            "Env var name that stores the SQLAlchemy URL",
            default="ATLAS_DB_URL",
            type=str,
        ).strip()
        if managed_env:
            env_values[url_env_var] = click.prompt(
                "Database SQLAlchemy URL",
                type=str,
                hide_input=True,
            ).strip()
        setup = DatabaseSetup(
            engine=engine,
            url_env_var=url_env_var,
            database="generic_target",
        )
    else:
        default_port = DatabaseEngine(engine).default_port or 0
        user_env_var = click.prompt(
            "Env var name for the database user",
            default="ATLAS_DB_USER",
            type=str,
        ).strip()
        password_env_var = click.prompt(
            "Env var name for the database password",
            default="ATLAS_DB_PASSWORD",
            type=str,
        ).strip()
        if managed_env:
            env_values[user_env_var] = click.prompt("Database user", type=str).strip()
            env_values[password_env_var] = click.prompt(
                "Database password",
                type=str,
                hide_input=True,
            ).strip()
        setup = DatabaseSetup(
            engine=engine,
            host=click.prompt("Database host", type=str, default="127.0.0.1").strip(),
            port=click.prompt("Database port", type=int, default=default_port),
            database=click.prompt("Database name", type=str).strip(),
            ssl_mode=click.prompt("SSL mode", type=str, default="disable").strip(),
            user_env_var=user_env_var,
            password_env_var=password_env_var,
        )

    setup.timeout_seconds = click.prompt(
        "Connection timeout (seconds)",
        type=int,
        default=30,
    )
    setup.sample_limit = click.prompt(
        "Sample limit per live query",
        type=int,
        default=50,
    )
    setup.privacy_mode = _privacy_prompt()
    setup.schema_filter = _split_csv(
        click.prompt(
            "Schema include list (comma-separated, blank for all)",
            type=str,
            default="",
            show_default=False,
        )
    )
    setup.schema_exclude = _split_csv(
        click.prompt(
            "Schema exclude list (comma-separated)",
            type=str,
            default="",
            show_default=False,
        )
    )
    return setup, env_values


def _collect_ai_setup(*, managed_env: bool) -> tuple[AISetup, dict[str, str]]:
    if not click.confirm("Enable local AI enrichment?", default=False):
        return AISetup(enabled=False), {}

    setup = AISetup(enabled=True)
    setup.provider = _ai_provider_prompt()
    if setup.provider == "ollama":
        setup.base_url = click.prompt(
            "Ollama base URL",
            type=str,
            default="http://127.0.0.1:11434",
        ).strip()
        setup.model = click.prompt("Model name", type=str, default="qwen2.5:1.5b").strip()
    elif setup.provider == "llamacpp":
        setup.base_url = click.prompt(
            "llama.cpp base URL",
            type=str,
            default="http://127.0.0.1:8080",
        ).strip()
        setup.model = click.prompt("Model label", type=str, default="local-model").strip()
    else:
        setup.base_url = click.prompt(
            "OpenAI-compatible local base URL",
            type=str,
            default="http://127.0.0.1:8000",
        ).strip()
        setup.model = click.prompt("Model name", type=str, default="qwen2.5:1.5b").strip()

    _ensure_local_ai_base_url(setup.base_url)
    setup.temperature = click.prompt("AI temperature", type=float, default=0.1)
    setup.max_tokens = click.prompt("AI max tokens", type=int, default=300)
    setup.timeout_seconds = click.prompt("AI timeout (seconds)", type=float, default=60.0)
    setup.parallel_workers = click.prompt("Parallel AI table workers", type=int, default=2)
    setup.column_mode = _column_mode_prompt()
    setup.force_recompute = click.confirm("Ignore semantic cache?", default=False)

    env_values: dict[str, str] = {}
    if setup.provider == "openai_compatible" and click.confirm(
        "Does this local gateway require an API key?",
        default=False,
    ):
        setup.api_key_env_var = click.prompt(
            "Env var name for the local AI API key",
            type=str,
            default="ATLAS_AI_API_KEY",
        ).strip()
        if managed_env:
            env_values[setup.api_key_env_var] = click.prompt(
                "API key",
                type=str,
                hide_input=True,
            ).strip()

    setup.selection_schemas = _split_csv(
        click.prompt(
            "Limit AI to schemas (comma-separated, blank for all)",
            type=str,
            default="",
            show_default=False,
        )
    )
    setup.selection_tables = _split_csv(
        click.prompt(
            "Limit AI to tables (schema.table, comma-separated)",
            type=str,
            default="",
            show_default=False,
        )
    )
    setup.selection_columns = _split_csv(
        click.prompt(
            "Limit AI to columns (schema.table.column, comma-separated)",
            type=str,
            default="",
            show_default=False,
        )
    )
    return setup, env_values


def _privacy_banner() -> None:
    click.echo(
        "[atlas onboard] This workflow keeps secrets and metadata on the local machine."
    )
    click.echo(
        "[atlas onboard] Atlas does not upload credentials, snapshots, samples, or schema "
        "metadata to third parties on its own."
    )
    click.echo(
        "[atlas onboard] Database traffic goes only to the database you configure. AI traffic "
        "goes only to a localhost/127.0.0.1 endpoint configured here."
    )


def _requires_env_secrets(database_setup: DatabaseSetup, ai_setup: AISetup) -> bool:
    return any(
        [
            bool(database_setup.url_env_var),
            bool(database_setup.user_env_var),
            bool(database_setup.password_env_var),
            bool(ai_setup.api_key_env_var),
        ]
    )


@click.command("onboard")
@click.option(
    "--resume",
    "resume_path",
    type=click.Path(path_type=Path),
    required=False,
    help="Reuse a saved atlas.onboard.json manifest and rerun the full pipeline.",
)
def onboard_cmd(resume_path: Path | None) -> None:
    """Interactive local-only onboarding for a full Atlas run."""

    if resume_path is not None:
        manifest = OnboardingManifest.load(require_existing_path(resume_path, kind="file"))
        click.echo(f"[atlas onboard] Reusing manifest {manifest.manifest_path}")
        outputs = run_onboarding(manifest, on_progress=click.echo)
        click.echo(json.dumps(outputs.to_dict(), ensure_ascii=False, indent=2))
        return

    _privacy_banner()
    workspace_dir = click.prompt(
        "Workspace directory for Atlas outputs",
        type=str,
        default="./atlas_onboarding",
    ).strip()
    project_name = click.prompt(
        "Project label",
        type=str,
        default="Atlas Onboarding Run",
    ).strip()
    generated_dir_name = click.prompt(
        "Generated artifacts directory name",
        type=str,
        default="generated",
    ).strip()
    sigilo_style = click.prompt(
        "Sigilo style",
        type=click.Choice(["network", "seal", "compact"], case_sensitive=False),
        default="network",
        show_choices=True,
    ).lower()
    sigilo_layout = click.prompt(
        "Sigilo layout",
        type=click.Choice(["circular", "force"], case_sensitive=False),
        default="circular",
        show_choices=True,
    ).lower()

    managed_env_preference = click.confirm(
        "Use a managed local .env file for any secrets entered in this wizard?",
        default=True,
    )

    database_setup, db_env_values = _collect_database_setup(managed_env=managed_env_preference)
    ai_setup, ai_env_values = _collect_ai_setup(managed_env=managed_env_preference)

    secrets_needed = _requires_env_secrets(database_setup, ai_setup)
    managed_env = managed_env_preference
    env_path = ".env"
    if secrets_needed:
        if managed_env:
            env_path = click.prompt(
                "Managed env file path",
                type=str,
                default=".env",
            ).strip()
        else:
            env_path = click.prompt(
                "Existing env file path",
                type=str,
                default=".env",
            ).strip()
    else:
        env_path = ".env"

    manifest = OnboardingManifest(
        project_name=project_name,
        workspace_dir=str(Path(workspace_dir).expanduser()),
        generated_dir_name=generated_dir_name,
        sigilo_style=sigilo_style,
        sigilo_layout=sigilo_layout,
        database=database_setup,
        ai=ai_setup,
        env_path=env_path,
        managed_env=managed_env,
    )

    if manifest.managed_env:
        _write_env_file(
            manifest.env_file_path,
            {**db_env_values, **ai_env_values},
        )

    if not click.confirm("Run the full Atlas onboarding pipeline now?", default=True):
        manifest.save()
        click.echo(f"[atlas onboard] Saved manifest to {manifest.manifest_path}")
        if manifest.managed_env:
            click.echo(f"[atlas onboard] Saved local env file to {manifest.env_file_path}")
        return

    outputs = run_onboarding(manifest, on_progress=click.echo)
    click.echo(json.dumps(outputs.to_dict(), ensure_ascii=False, indent=2))
