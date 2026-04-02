"""Integration coverage for Phase 0B configuration loading workflows."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.phase_0b]


def test_from_url_in_subprocess_returns_masked_password(
    run_command,
    repo_root,
    python_executable: str,
) -> None:
    code = (
        "from atlas.config import AtlasConnectionConfig;"
        "cfg = AtlasConnectionConfig.from_url('postgresql://app:secret@localhost:5432/mydb');"
        "print(cfg.to_json())"
    )
    result = run_command([python_executable, "-c", code], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["engine"] == "postgresql"
    assert payload["password"] == "***"
    assert payload["port"] == 5432


def test_from_file_in_subprocess_reads_analysis_section(
    run_command,
    repo_root,
    phase_tmp_dir: Path,
    python_executable: str,
) -> None:
    config_path = phase_tmp_dir / "atlas.toml"
    config_path.write_text(
        textwrap.dedent(
            """
            [connection]
            engine = "mysql"
            host = "db.internal"
            database = "shop"

            [connection.connect_args]
            charset = "utf8mb4"

            [analysis]
            sample_limit = 25
            privacy_mode = "masked"
            schema_filter = ["sales"]
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    code = (
        "from atlas.config import AtlasConnectionConfig;"
        f"cfg = AtlasConnectionConfig.from_file({config_path.as_posix()!r});"
        "print(cfg.to_json())"
    )
    result = run_command([python_executable, "-c", code], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["engine"] == "mysql"
    assert payload["privacy_mode"] == "masked"
    assert payload["schema_filter"] == ["sales"]
    assert payload["connect_args"] == {"charset": "utf8mb4"}


def test_from_env_in_subprocess_reads_schema_filters(
    run_command,
    repo_root,
    python_executable: str,
) -> None:
    env = {
        "ATLAS_ENGINE": "postgresql",
        "ATLAS_HOST": "db.example.com",
        "ATLAS_DATABASE": "mydb",
        "ATLAS_SCHEMA_FILTER": "public, app",
        "ATLAS_SCHEMA_EXCLUDE": "pg_catalog",
    }
    code = (
        "from atlas.config import AtlasConnectionConfig;"
        "cfg = AtlasConnectionConfig.from_env();"
        "print(cfg.to_json())"
    )
    result = run_command([python_executable, "-c", code], cwd=repo_root, env=env)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["schema_filter"] == ["public", "app"]
    assert payload["schema_exclude"] == ["pg_catalog"]


def test_json_roundtrip_in_subprocess_preserves_password_when_requested(
    run_command,
    repo_root,
    python_executable: str,
) -> None:
    code = (
        "from atlas.config import AtlasConnectionConfig;"
        "cfg = AtlasConnectionConfig.from_url('postgresql://app:secret@localhost/mydb');"
        "restored = AtlasConnectionConfig.from_json(cfg.to_json(include_password=True));"
        "print(restored.to_json(include_password=True))"
    )
    result = run_command([python_executable, "-c", code], cwd=repo_root)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["password"] == "secret"
    assert payload["user"] == "app"


def test_invalid_env_in_subprocess_returns_nonzero(
    run_command,
    repo_root,
    python_executable: str,
) -> None:
    env = {
        "ATLAS_ENGINE": "postgresql",
        "ATLAS_HOST": "localhost",
        "ATLAS_DATABASE": "mydb",
        "ATLAS_TIMEOUT": "abc",
    }
    code = textwrap.dedent(
        """
        from atlas.config import AtlasConnectionConfig, ConfigValidationError
        try:
            AtlasConnectionConfig.from_env()
        except ConfigValidationError as exc:
            raise SystemExit(str(exc))
        """
    )
    result = run_command([python_executable, "-c", code], cwd=repo_root, env=env)
    assert result.returncode != 0
    assert "must be an integer" in result.stderr
