"""Phase 14A integration tests for the public Atlas SDK facade."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas import AIConnectionError, Atlas, AtlasConnectionConfig, IntrospectionResult
from atlas.ai import AIConfig, LocalLLMClient, SemanticCache
from atlas.sdk import AtlasSigiloArtifact
from tests.integration.phase_14.helpers import (
    DeterministicLocalClient,
    create_phase14_sqlite_db,
    make_phase14_config,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_14a]


@pytest.fixture()
def sdk_sqlite_path(phase_tmp_dir: Path) -> Path:
    return create_phase14_sqlite_db(phase_tmp_dir / "sdk_public_api.db")


@pytest.fixture()
def sdk_config(sdk_sqlite_path: Path) -> AtlasConnectionConfig:
    return make_phase14_config(sdk_sqlite_path)


def test_sdk_scan_introspects_sqlite_via_public_api(sdk_config: AtlasConnectionConfig) -> None:
    atlas = Atlas(sdk_config)

    result = atlas.scan()

    assert isinstance(result, IntrospectionResult)
    assert result.engine == "sqlite"
    assert result.get_table("main", "customers") is not None
    assert result.get_table("main", "orders") is not None


def test_sdk_build_sigilo_returns_savable_artifact(sdk_config: AtlasConnectionConfig) -> None:
    atlas = Atlas(sdk_config)
    result = atlas.scan()

    artifact = atlas.build_sigilo(result, style="compact")

    assert isinstance(artifact, AtlasSigiloArtifact)
    assert artifact.svg_bytes.startswith(b"<svg")
    assert "customers" in artifact.to_svg_text()


def test_sdk_sigilo_artifact_save_writes_svg_file(
    sdk_config: AtlasConnectionConfig,
    phase_tmp_dir: Path,
) -> None:
    atlas = Atlas(sdk_config)
    result = atlas.scan()
    artifact = atlas.build_sigilo(result, style="network")

    saved_path = artifact.save(phase_tmp_dir / "artifacts")

    assert saved_path.exists()
    assert saved_path.suffix == ".svg"
    assert saved_path.read_text(encoding="utf-8").startswith("<svg")


def test_sdk_create_snapshot_roundtrip_works_with_public_artifact(
    sdk_config: AtlasConnectionConfig,
    phase_tmp_dir: Path,
) -> None:
    atlas = Atlas(sdk_config)
    result = atlas.scan()
    artifact = atlas.build_sigilo(result)

    snapshot = atlas.create_snapshot(result, artifact)
    saved = snapshot.save(phase_tmp_dir / "sdk_snapshot")

    assert saved.exists()
    loaded = snapshot.load(saved)
    assert loaded.result.database == result.database
    assert "customers" in loaded.sigil_svg


def test_sdk_save_scan_artifacts_writes_all_canonical_outputs(
    sdk_config: AtlasConnectionConfig,
    phase_tmp_dir: Path,
) -> None:
    atlas = Atlas(sdk_config)
    result = atlas.scan()
    artifact = atlas.build_sigilo(result)

    outputs = atlas.save_scan_artifacts(result, artifact, phase_tmp_dir)

    assert outputs.svg_path.exists()
    assert outputs.sigil_path.exists()
    assert outputs.meta_json_path.exists()


def test_sdk_detect_local_llm_raises_stable_runtime_error_on_unreachable_provider(
    sdk_config: AtlasConnectionConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    atlas = Atlas(sdk_config)

    def _boom(_: AIConfig) -> LocalLLMClient:
        raise AIConnectionError("offline")

    monkeypatch.setattr("atlas.sdk.auto_detect_client", _boom)

    with pytest.raises(RuntimeError, match="No local LLM provider is reachable."):
        atlas.detect_local_llm()


def test_sdk_enrich_mutates_result_in_place_with_fake_client(
    sdk_config: AtlasConnectionConfig,
    phase_tmp_dir: Path,
) -> None:
    atlas = Atlas(sdk_config)
    result = atlas.scan()
    cache = SemanticCache(phase_tmp_dir / ".semantic_cache")

    enriched = atlas.enrich(
        result,
        client=DeterministicLocalClient(),
        cache=cache,
        tables_only=True,
    )

    orders = enriched.get_table("main", "orders")
    assert orders is not None
    assert enriched is result
    assert orders.semantic_short == "Customer orders"
    assert orders.semantic_domain == "sales"


def test_sdk_ask_answers_without_cli(
    sdk_config: AtlasConnectionConfig,
) -> None:
    atlas = Atlas(sdk_config)
    result = atlas.scan()

    answer = atlas.ask(
        result,
        "where are customer orders stored?",
        client=DeterministicLocalClient(),
    )

    assert answer.candidates
    assert answer.candidates[0].qualified_name == "main.orders"
    assert answer.confidence > 0.0
