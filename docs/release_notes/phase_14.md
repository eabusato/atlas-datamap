# Phase 14 Release Notes

## Scope

Phase 14 closes Step 1 with a stable public SDK, explicit regression baselines,
and release-ready distribution metadata.

## Delivered

- Added the public SDK facade in
  [`atlas/sdk.py`](../../atlas/sdk.py) with:
  - `Atlas`
  - `AtlasSigiloArtifact`
  - high-level `scan`, `build_sigilo`, `save_scan_artifacts`,
    `create_snapshot`, `detect_local_llm`, `enrich`, and `ask`
- Exported the SDK surface from
  [`atlas/__init__.py`](../../atlas/__init__.py)
- Added shared Phase 14 fixtures in
  [`tests/integration/phase_14/helpers.py`](../../tests/integration/phase_14/helpers.py)
- Added SVG baseline normalization in
  [`tests/support/svg_baseline.py`](../../tests/support/svg_baseline.py)
- Added approved sigilo baselines in
  [`tests/baselines/phase_14/`](../../tests/baselines/phase_14)
- Added regression coverage in
  [`tests/integration/phase_14/test_regression_14b.py`](../../tests/integration/phase_14/test_regression_14b.py)
- Added local release-packaging coverage in
  [`tests/integration/phase_14/test_distribution_14c.py`](../../tests/integration/phase_14/test_distribution_14c.py)
- Extended the unified runner in
  [`tests/run_tests.sh`](../../tests/run_tests.sh)
- Extended CI in
  [`build.yml`](../../.github/workflows/build.yml)
- Added release metadata and publishing docs:
  - [`CHANGELOG.md`](../../CHANGELOG.md)
  - [`docs/publishing.md`](../publishing.md)
  - [`publish.yml`](../../.github/workflows/publish.yml)
  - [`MANIFEST.in`](../../MANIFEST.in)

## Notes

- Phase 14 preserves the approved sigilo visual formulation. The renderer
  itself was not changed in this block.
- The publish workflow is prepared but not executed as part of local phase
  validation.
- The release surface now targets `1.0.0`, matching the end of Step 1.
