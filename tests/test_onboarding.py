"""Unit tests for Atlas onboarding helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from atlas.introspection.runner import _ProgressEvent
from atlas.onboarding import (
    AISetup,
    DatabaseSetup,
    OnboardingManifest,
    _build_selection_plan,
    _parse_env_file,
    _render_scan_progress,
    _write_env_file,
    _write_reference_files,
)


def test_write_and_parse_env_file_round_trip(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    _write_env_file(
        env_path,
        {
            "ATLAS_DB_USER": "analyst",
            "ATLAS_DB_PASSWORD": "topsecret",
        },
    )

    payload = _parse_env_file(env_path)
    assert payload["ATLAS_DB_USER"] == "analyst"
    assert payload["ATLAS_DB_PASSWORD"] == "topsecret"


def test_onboarding_manifest_persists_only_env_var_refs(tmp_path: Path) -> None:
    manifest = OnboardingManifest(
        project_name="Local Run",
        workspace_dir=str(tmp_path),
        database=DatabaseSetup(
            engine="postgresql",
            host="127.0.0.1",
            port=5432,
            database="atlas_demo",
            user_env_var="ATLAS_DB_USER",
            password_env_var="ATLAS_DB_PASSWORD",
        ),
        ai=AISetup(enabled=False),
        env_path=".env",
        managed_env=True,
    )

    manifest.save()
    payload = json.loads(manifest.manifest_path.read_text(encoding="utf-8"))
    rendered = json.dumps(payload)
    assert "ATLAS_DB_PASSWORD" in rendered
    assert "topsecret" not in rendered


def test_ai_setup_rejects_non_local_endpoints() -> None:
    ai = AISetup(
        enabled=True,
        provider="openai_compatible",
        model="mini",
        base_url="https://api.example.com/v1",
    )

    with pytest.raises(ValueError, match="local AI endpoints"):
        ai.build_config({})


def test_selection_plan_builds_schema_table_and_column_maps() -> None:
    plan = _build_selection_plan(
        AISetup(
            enabled=True,
            selection_schemas=["core"],
            selection_tables=["risk.alerts"],
            selection_columns=["risk.cases.status", "risk.cases.opened_at"],
        )
    )

    assert plan is not None
    assert plan.schema_names == {"core", "risk"}
    assert plan.tables_by_schema["risk"] == {"alerts", "cases"}
    assert plan.columns_by_table[("risk", "cases")] == {"status", "opened_at"}


def test_onboarding_connection_reference_includes_env_file(tmp_path: Path) -> None:
    manifest = OnboardingManifest(
        project_name="Local Run",
        workspace_dir=str(tmp_path),
        database=DatabaseSetup(
            engine="mysql",
            host="db.internal",
            database="atlas_demo",
            user_env_var="ATLAS_DB_USER",
            password_env_var="ATLAS_DB_PASSWORD",
        ),
        ai=AISetup(enabled=False),
        env_path=".env",
        managed_env=True,
    )

    manifest.save()
    rendered = manifest.connection_reference_path

    _write_reference_files(manifest, include_ai=False, selection_plan=None)
    content = rendered.read_text(encoding="utf-8")
    assert 'env_file = ".env"' in content


def test_render_scan_progress_formats_stage_progress() -> None:
    message = _render_scan_progress(
        _ProgressEvent(
            stage="columns",
            message="Loading columns for main.payments",
            current=2,
            total=5,
            elapsed_ms=123,
        )
    )

    assert message == (
        "[atlas onboard] scan columns 2/5 [123 ms] "
        "Loading columns for main.payments"
    )
