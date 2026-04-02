# Phase 6 Release Notes

## Scope

Phase 6 adds deterministic structural analysis on top of Atlas metadata:

- heuristic table classification
- relevance scoring and ranking
- anomaly detection

## Delivered

- Added the new `atlas.analysis` package with public classifier, scorer, and
  anomaly APIs.
- Implemented `TableClassifier` with deterministic signal weights for
  `staging`, `config`, `pivot`, `log`, `fact`, `domain_main`, `dimension`, and
  `unknown`.
- Implemented `TableScorer` with weighted volume, connectivity, fill rate,
  index, name, and comment scoring, plus table ranking and domain clustering.
- Implemented `AnomalyDetector` with structural checks for missing indexes,
  missing PKs, high-nullability tables without PKs, ambiguous columns,
  unindexed foreign keys, empty tables, implicit foreign keys, and wide tables.
- Exported the analysis layer from both `atlas.analysis` and the package root.
- Added unit and integration suites for 6A, 6B, and 6C, including a real
  SQLite fixture used across the phase.
- Wired Phase 6 into `tests/run_tests.sh` and registered the new pytest
  markers.

## Notes

- Phase 6 analysis is intentionally heuristic and metadata-driven; it does not
  inspect SQL workloads or query logs.
- Score quality depends on the metadata richness provided by each connector,
  especially comments, explicit indexes, and column statistics.
