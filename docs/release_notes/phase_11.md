# Phase 11 Release Notes

## Scope

Phase 11 adds persistent offline snapshots, structural snapshot diffs, and a
local history workflow for Atlas archives.

## Delivered

- Added `SnapshotManifest` and `AtlasSnapshot` in
  [`atlas/export/snapshot.py`](../../atlas/export/snapshot.py)
  with:
  - `.atlas` ZIP persistence
  - `peek_manifest(...)`
  - reload of structure, sigilo, scores, anomalies, and optional semantics
- Kept the existing scan artifact helpers intact while extending the module for
  snapshot archives
- Added `SnapshotDiff`, `SchemaDiff`, `ColumnTypeChange`, and `VolumeChange` in
  [`atlas/export/diff.py`](../../atlas/export/diff.py)
- Added HTML diff rendering in
  [`atlas/export/diff_report.py`](../../atlas/export/diff_report.py)
  using side-by-side stored sigilos without changing their visual contract
- Added the real `atlas diff` command in
  [`atlas/cli/diff.py`](../../atlas/cli/diff.py)
- Added `AtlasHistory` in
  [`atlas/history.py`](../../atlas/history.py)
  with:
  - deterministic snapshot naming
  - newest-first listing
  - `latest` resolution
  - date-prefix resolution with ambiguity checks
- Added the real `atlas history` command group in
  [`atlas/cli/history.py`](../../atlas/cli/history.py)
  with:
  - `list`
  - `diff`
  - `open`
- Updated the root CLI, public exports, pytest markers, and the unified test
  runner for Phase 11
- Expanded historical packaging coverage so `atlas diff --help` and
  `atlas history --help` are treated as real public command surfaces

## Notes

- Phase 11 does not modify the approved sigilo visual formulation.
- `.atlas` snapshots are offline artifacts; they do not reconnect to the
  source database on load.
- Diff rendering reuses stored sigilos side by side rather than introducing a
  new visual grammar.
