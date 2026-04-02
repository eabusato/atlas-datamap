# Phase 12 Release Notes

## Scope

Phase 12 adds offline sigilo sharing, structured metadata exports, and an
executive HTML reporting mode.

## Delivered

- Added standalone HTML export in
  [`atlas/export/standalone.py`](../../atlas/export/standalone.py)
  with:
  - inline CSS and JavaScript
  - embedded canonical SVG
  - a clickable table browser and detail panel
  - zero external assets
- Added structured export support in
  [`atlas/export/structured.py`](../../atlas/export/structured.py)
  for:
  - JSON
  - CSV tables
  - CSV columns
  - Markdown data dictionaries
- Added the new `atlas export` command group in
  [`atlas/cli/export.py`](../../atlas/cli/export.py)
  with:
  - `atlas export svg`
  - `atlas export json`
  - `atlas export csv`
  - `atlas export markdown`
- Extended [`atlas/cli/report.py`](../../atlas/cli/report.py)
  to accept:
  - `--atlas`
  - `--style health|executive`
- Added executive reporting in
  [`atlas/export/report_executive.py`](../../atlas/export/report_executive.py)
  with overview, schema inventory, top tables, anomaly summaries,
  deterministic recommendations, and optional semantic coverage
- Updated public exports, CLI registration, pytest markers, and the unified
  test runner for Phase 12
- Added integration coverage in
  [`tests/integration/phase_12/`](../../tests/integration/phase_12)

## Notes

- Phase 12 preserves the approved sigilo visual formulation by wrapping or
  reusing the existing SVG instead of changing the renderer grammar.
- Structured exports use the real Atlas snapshot semantics shape:
  `tables` and `columns` maps, not a flat semantic payload.
- `atlas report --style health` remains backward-compatible with the Phase 7
  health report contract.
