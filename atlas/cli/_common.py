"""Shared CLI helpers for Atlas command implementations."""

from __future__ import annotations

from pathlib import Path

import click

from atlas.config import AtlasConnectionConfig, ConfigValidationError, PrivacyMode


def resolve_config(
    *,
    db: str | None,
    config_path: str | None,
    schema_csv: str | None = None,
    privacy: str | None = None,
) -> AtlasConnectionConfig:
    """Resolve configuration from file, URL, or environment, then apply CLI overrides."""

    try:
        if config_path:
            config = AtlasConnectionConfig.from_file(config_path)
        elif db:
            config = AtlasConnectionConfig.from_url(db)
        else:
            config = AtlasConnectionConfig.from_env()
    except ConfigValidationError as exc:
        if not db and not config_path:
            raise click.UsageError(
                "Provide --db <URL>, --config <path>, or ATLAS_* environment variables."
            ) from exc
        raise click.UsageError(str(exc)) from exc

    overrides = config.to_dict(include_password=True)
    schema_filter = parse_csv_list(schema_csv)
    if schema_filter:
        overrides["schema_filter"] = schema_filter
    if privacy is not None:
        overrides["privacy_mode"] = PrivacyMode(privacy)
    return AtlasConnectionConfig.from_dict(overrides)


def parse_csv_list(value: str | None) -> list[str]:
    """Split a comma-separated CLI argument into normalized tokens."""

    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def require_existing_path(path: str | Path, *, kind: str = "file") -> Path:
    """Return a validated existing path or raise a Click usage error."""

    resolved = Path(path)
    if kind == "file":
        if not resolved.is_file():
            raise click.UsageError(f"{resolved} is not an existing file.")
    elif kind == "dir":
        if not resolved.is_dir():
            raise click.UsageError(f"{resolved} is not an existing directory.")
    else:  # pragma: no cover - defensive contract
        raise ValueError(f"Unsupported path kind {kind!r}.")
    return resolved
