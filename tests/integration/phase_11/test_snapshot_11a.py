"""Integration tests for Phase 11A snapshot archives."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from atlas.export.snapshot import AtlasSnapshot
from atlas.version import ATLAS_VERSION
from tests.integration.phase_11.helpers import (
    build_phase11_plain_result,
    build_phase11_semantic_result,
    build_snapshot_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_11a]


def test_snapshot_roundtrip_preserves_required_payloads(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "snapshot.db"
    result = build_phase11_semantic_result(db_path)
    snapshot = build_snapshot_fixture(result)

    target = phase_tmp_dir / "aurora_bank"
    saved_path = snapshot.save(target)
    assert saved_path.suffix == ".atlas"
    assert saved_path.exists()

    loaded = AtlasSnapshot.load(saved_path)
    assert loaded.result.database == result.database
    assert loaded.result.get_table("main", "fact_orders") is not None
    assert loaded.manifest.database == result.database
    assert loaded.manifest.has_semantics is True
    assert loaded.manifest.has_scores is True
    assert loaded.manifest.has_anomalies is True
    assert loaded.sigil_svg.startswith("<svg")
    assert loaded.sigil_payload.startswith("{")
    assert any(item["table"] == "fact_orders" for item in loaded.scores)
    assert any(item["table"] == "config_settings" for item in loaded.anomalies)


def test_snapshot_without_semantics_loads_cleanly(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "plain.db"
    result = build_phase11_plain_result(db_path)
    snapshot = build_snapshot_fixture(result)

    path = snapshot.save(phase_tmp_dir / "plain.atlas")
    loaded = AtlasSnapshot.load(path)
    assert loaded.semantics is None
    assert loaded.manifest.has_semantics is False
    assert loaded.result.total_tables >= 1


def test_peek_manifest_reads_header_only(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "peek.db"
    result = build_phase11_semantic_result(db_path)
    snapshot = build_snapshot_fixture(result)
    path = snapshot.save(phase_tmp_dir / "peek")

    manifest = AtlasSnapshot.peek_manifest(path)
    assert manifest.database == result.database
    assert manifest.engine == result.engine
    assert manifest.format_version == "1.0"
    assert "schema.json" in manifest.contents
    assert "sigilo.svg" in manifest.contents


def test_load_fails_on_corrupted_zip(phase_tmp_dir: Path) -> None:
    path = phase_tmp_dir / "broken.atlas"
    path.write_text("not a zip archive", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid or corrupted"):
        AtlasSnapshot.load(path)


def test_load_fails_when_required_file_is_missing(phase_tmp_dir: Path) -> None:
    path = phase_tmp_dir / "missing.atlas"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "manifest.json",
            json.dumps(
                {
                    "format_version": "1.0",
                    "atlas_version": ATLAS_VERSION,
                    "created_at": "2026-04-02T10:00:00Z",
                    "database": "atlas_cli",
                    "engine": "sqlite",
                    "schema_count": 1,
                    "table_count": 1,
                    "has_semantics": False,
                    "has_anomalies": True,
                    "has_scores": True,
                    "contents": ["manifest.json"],
                }
            ),
        )

    with pytest.raises(ValueError, match="missing required files"):
        AtlasSnapshot.load(path)


def test_save_enforces_atlas_extension(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "extension.db"
    result = build_phase11_semantic_result(db_path)
    snapshot = build_snapshot_fixture(result)

    saved = snapshot.save(phase_tmp_dir / "snapshot_name")
    assert saved.name == "snapshot_name.atlas"


def test_snapshot_preserves_svg_and_sigil_text_verbatim(phase_tmp_dir: Path) -> None:
    db_path = phase_tmp_dir / "verbatim.db"
    result = build_phase11_semantic_result(db_path)
    snapshot = build_snapshot_fixture(result)
    path = snapshot.save(phase_tmp_dir / "verbatim.atlas")

    loaded = AtlasSnapshot.load(path)
    assert loaded.sigil_svg == snapshot.sigil_svg
    assert loaded.sigil_payload == snapshot.sigil_payload
