"""Helpers shared by Phase 11 integration tests."""

from __future__ import annotations

import json
from pathlib import Path

from atlas.export.snapshot import AtlasSnapshot
from atlas.sigilo.builder import SigiloBuilder
from atlas.types import IntrospectionResult
from tests.integration.phase_7.helpers import (
    build_phase7_sqlite_fixture,
    introspect_phase7_sqlite,
)
from tests.integration.phase_10.helpers import build_phase10_result


def build_phase11_plain_result(db_path: Path) -> IntrospectionResult:
    """Create a structural result without semantic enrichment."""

    build_phase7_sqlite_fixture(db_path)
    return introspect_phase7_sqlite(db_path)


def build_phase11_semantic_result(db_path: Path) -> IntrospectionResult:
    """Create a deterministic result with semantic fields populated."""

    return build_phase10_result(db_path)


def build_snapshot_fixture(result: IntrospectionResult) -> AtlasSnapshot:
    """Create a realistic snapshot using the current sigilo builder."""

    sigil_payload = json.dumps(result.to_dict(), ensure_ascii=False, separators=(",", ":"))
    sigil_svg = SigiloBuilder(result, prefer_native=False).build_svg().decode("utf-8")
    return AtlasSnapshot.from_result(
        result,
        sigil_svg=sigil_svg,
        sigil_payload=sigil_payload,
    )
