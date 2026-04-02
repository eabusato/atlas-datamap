"""Helpers shared by Phase 12 integration tests."""

from __future__ import annotations

import json
from pathlib import Path

from atlas.export.snapshot import AtlasSnapshot
from atlas.sigilo.builder import SigiloBuilder
from atlas.types import IntrospectionResult
from tests.integration.phase_10.helpers import build_phase10_result


def build_phase12_result(db_path: Path) -> IntrospectionResult:
    """Create a deterministic semantic result suitable for export tests."""

    return build_phase10_result(db_path)


def write_sigil_fixture(base_dir: Path, result: IntrospectionResult, name: str = "fixture") -> Path:
    """Persist one .sigil payload inside the test repository tree."""

    target = base_dir / f"{name}.sigil"
    target.write_text(json.dumps(result.to_dict(), ensure_ascii=False), encoding="utf-8")
    return target


def build_phase12_snapshot(result: IntrospectionResult) -> AtlasSnapshot:
    """Create a realistic snapshot with the canonical current sigilo."""

    sigil_payload = json.dumps(result.to_dict(), ensure_ascii=False, separators=(",", ":"))
    sigil_svg = SigiloBuilder(result).build_svg().decode("utf-8")
    return AtlasSnapshot.from_result(
        result,
        sigil_svg=sigil_svg,
        sigil_payload=sigil_payload,
    )


def write_snapshot_fixture(base_dir: Path, result: IntrospectionResult, name: str = "fixture") -> Path:
    """Persist one .atlas snapshot inside the test repository tree."""

    snapshot = build_phase12_snapshot(result)
    return snapshot.save(base_dir / name)
