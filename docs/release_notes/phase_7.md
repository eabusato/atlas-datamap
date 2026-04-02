# Phase 7 Release Notes

## Scope

Phase 7 adds search, heuristic discovery, and a shareable HTML reporting
surface on top of the metadata and analysis stack from earlier phases.

## Delivered

- Added the `atlas.search` package with canonical search result types, a
  deterministic in-memory search engine, and heuristic domain discovery.
- Implemented `atlas search` with mixed schema/table/column search, schema
  filtering, table-type filtering, and column-only mode.
- Implemented `AtlasDiscovery` with bilingual stop-word removal, domain
  synonym expansion, candidate score accumulation, FK hub bonus, reasoning
  text, and normalized confidence.
- Implemented `HTMLReportGenerator` for stand-alone HTML health reports with
  structural summary, ranking sections, heuristic type distribution, anomaly
  listings, and embedded sigilo support.
- Implemented `atlas report` with live-database mode, `.sigil` snapshot mode,
  output-path control, and `--no-sigilo` fallback behavior.
- Added unit and integration coverage for 7A, 7B, and 7C, including a real
  SQLite fixture shared across the phase.
- Wired Phase 7 into `tests/run_tests.sh` and extended the phase index and
  manuals.

## Notes

- Phase 7 preserves the current sigilo rendering contract and does not alter
  the visual formulation of the SVG output.
- The report intentionally degrades to a textual warning when the native sigilo
  binding is unavailable instead of silently switching visual engines.
