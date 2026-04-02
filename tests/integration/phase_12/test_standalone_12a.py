"""Integration tests for Phase 12A standalone HTML sigilo export."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas.cli.export import load_export_source
from atlas.export.snapshot import AtlasSnapshot
from atlas.export.standalone import StandaloneHTMLBuilder
from tests.integration.phase_12.helpers import (
    build_phase12_result,
    build_phase12_snapshot,
    write_sigil_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.phase_12a]


def _run_export(
    run_command,
    repo_root: Path,
    python_executable: str,
    *args: str,
):
    return run_command([python_executable, "-m", "atlas", "export", *args], cwd=repo_root)


def test_standalone_builder_embeds_complete_html_document(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "full.db")
    source = load_export_source(sigil_path=write_sigil_fixture(phase_tmp_dir, result), atlas_path=None)

    html = StandaloneHTMLBuilder(
        source.svg_content or "",
        db_name=result.database,
        has_semantics=False,
    ).build_html()

    assert html.startswith("<!DOCTYPE html>")
    assert "<meta charset=\"UTF-8\"/>" in html
    assert "<svg" in html
    assert "Atlas Standalone Sigilo" in html
    assert "data-atlas-zoom-out" in html
    assert "data-atlas-zoom-fit" in html
    assert ".atlas-canvas .atlas-zoom-shell { flex: 1; min-height: calc(100vh - 28px); }" in html
    assert ".atlas-canvas .atlas-zoom-viewport { height: 100%; min-height: 0; }" in html


def test_standalone_builder_has_no_external_dependencies(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "offline.db")
    source = load_export_source(sigil_path=write_sigil_fixture(phase_tmp_dir, result), atlas_path=None)
    html = StandaloneHTMLBuilder(source.svg_content or "", db_name=result.database).build_html()

    assert "<script src=" not in html
    assert "<link " not in html
    assert "@import url(" not in html
    assert "cdn.jsdelivr.net" not in html
    assert "cdnjs.cloudflare.com" not in html


def test_standalone_builder_preserves_structural_and_semantic_data_attrs(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "semantic.db")
    source = load_export_source(sigil_path=write_sigil_fixture(phase_tmp_dir, result), atlas_path=None)
    html = StandaloneHTMLBuilder(source.svg_content or "", db_name=result.database).build_html()

    assert "data-row-estimate=" in html
    assert "data-size-bytes=" in html
    assert "data-column-count=" in html
    assert "data-semantic-short=" in html
    assert "data-semantic-role=" in html


def test_standalone_builder_uses_canonical_wrapper_selectors(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "selectors.db")
    source = load_export_source(sigil_path=write_sigil_fixture(phase_tmp_dir, result), atlas_path=None)
    html = StandaloneHTMLBuilder(source.svg_content or "", db_name=result.database).build_html()

    assert ".system-node-wrap[data-table]" in html
    assert ".system-schema-wrap[data-schema]" in html
    assert ".system-edge-wrap[data-fk-from]" in html


def test_cli_export_svg_from_sigil_generates_valid_html(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    result = build_phase12_result(phase_tmp_dir / "cli_sigil.db")
    sigil_path = write_sigil_fixture(phase_tmp_dir, result, name="input")
    output_path = phase_tmp_dir / "standalone.html"

    command = _run_export(
        run_command,
        repo_root,
        python_executable,
        "svg",
        "--sigil",
        str(sigil_path),
        "--output",
        str(output_path),
    )

    assert command.returncode == 0, command.stderr
    assert output_path.exists()
    content = output_path.read_text(encoding="utf-8")
    assert "<svg" in content
    assert "fact_orders" in content


def test_cli_export_svg_from_atlas_reuses_snapshot_svg(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    result = build_phase12_result(phase_tmp_dir / "cli_atlas.db")
    snapshot = build_phase12_snapshot(result)
    snapshot.sigil_svg = snapshot.sigil_svg.replace("<svg", "<svg data-export-origin=\"snapshot\"", 1)
    atlas_path = snapshot.save(phase_tmp_dir / "fixture")
    output_path = phase_tmp_dir / "standalone_from_atlas.html"

    command = _run_export(
        run_command,
        repo_root,
        python_executable,
        "svg",
        "--atlas",
        str(atlas_path),
        "--output",
        str(output_path),
    )

    assert command.returncode == 0, command.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "data-export-origin=\"snapshot\"" in content
    assert "<svg" in content


def test_standalone_export_detects_snapshot_semantics(phase_tmp_dir: Path) -> None:
    result = build_phase12_result(phase_tmp_dir / "has_semantics.db")
    atlas_path = build_phase12_snapshot(result).save(phase_tmp_dir / "with_semantics")
    snapshot = AtlasSnapshot.load(atlas_path)

    html = StandaloneHTMLBuilder(
        snapshot.sigil_svg,
        db_name=snapshot.manifest.database,
        has_semantics=bool(snapshot.semantics),
    ).build_html()

    assert "Semantic metadata detected." in html


def test_cli_export_svg_can_strip_semantics_from_snapshot(
    run_command,
    repo_root: Path,
    python_executable: str,
    phase_tmp_dir: Path,
) -> None:
    result = build_phase12_result(phase_tmp_dir / "no_semantics.db")
    atlas_path = build_phase12_snapshot(result).save(phase_tmp_dir / "without_semantics")
    output_path = phase_tmp_dir / "standalone_no_semantics.html"

    command = _run_export(
        run_command,
        repo_root,
        python_executable,
        "svg",
        "--atlas",
        str(atlas_path),
        "--output",
        str(output_path),
        "--no-semantics",
    )

    assert command.returncode == 0, command.stderr
    content = output_path.read_text(encoding="utf-8")
    assert "Structural metadata only." in content
    assert "data-semantic-short=" not in content
    assert "data-semantic-role=" not in content
