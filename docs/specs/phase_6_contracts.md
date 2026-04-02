# Phase 6 Contracts

## Analysis package contract

Phase 6 introduces the heuristic analysis package:

- [`atlas/analysis/classifier.py`](../../atlas/analysis/classifier.py)
- [`atlas/analysis/scorer.py`](../../atlas/analysis/scorer.py)
- [`atlas/analysis/anomalies.py`](../../atlas/analysis/anomalies.py)
- [`atlas/analysis/__init__.py`](../../atlas/analysis/__init__.py)

Public exports now include:

- `TableClassifier`
- `TableClassification`
- `TableScorer`
- `TableScore`
- `ScoreBreakdown`
- `AnomalyDetector`
- `StructuralAnomaly`
- `AnomalySeverity`

The package is deterministic and does not use network calls, LLMs, or external
services.

## Phase 6A classification contract

`TableClassifier` classifies each `TableInfo` into one of:

- `staging`
- `config`
- `pivot`
- `log`
- `fact`
- `domain_main`
- `dimension`
- `unknown`

`classify(table, fk_in_degree)` returns `TableClassification(table, schema,
probable_type, confidence, signals)`.

`classify_all(result)`:

- iterates every table from `IntrospectionResult.all_tables()`
- computes inbound FK degree from `result.fk_in_degree_map`
- mutates `table.heuristic_type`
- mutates `table.heuristic_confidence`
- returns the complete list of `TableClassification`

Signal rules implemented in Phase 6A:

- staging: staging-style prefixes/suffixes, very small row counts, ETL/landing
  column patterns
- config: config-style names, small lookup row counts, key/value column pairs
- pivot: two or three outbound foreign keys, join-table PK structures, few
  non-key columns, relationship-oriented names
- log: datetime density, event/action column names, log/audit names, and lack
  of `updated_at`
- fact: three or more outbound foreign keys, measure-like numeric columns,
  timestamp columns, fact-style names
- domain_main: high inbound FK degree, large row count, explicit primary key,
  and non-log/non-staging shape
- dimension: primary key, low outbound FK count, mostly descriptive text
  attributes, dimension-style names, and classic entity shape

Confidence rules:

- confidence is the active signal sum divided by the total configured weight
  for the winning type
- ties are broken by specificity order:
  `staging > log > fact > pivot > dimension > config > domain_main`
- if the winning confidence is below `0.3`, the final type is `unknown`

Operational limits:

- classification depends on metadata quality already present in `TableInfo`
- comments, sample rows, and semantic enrichment are not used in Phase 6A
- engines that under-report foreign keys or table sizes can reduce confidence

## Phase 6B relevance scoring contract

`TableScorer(result)` computes per-table relevance through weighted heuristics.

`ScoreBreakdown` contains:

- `volume_score`
- `connectivity_score`
- `fill_rate_score`
- `index_score`
- `name_score`
- `comment_score`
- `total`

Weights are fixed:

- volume: `0.30`
- connectivity: `0.30`
- fill rate: `0.15`
- indexes: `0.10`
- name: `0.10`
- comment: `0.05`

Implemented scoring rules:

- volume: bucketed from `0.0` to `1.0` by `row_count_estimate`
- connectivity: `min(1.0, (fk_in_degree + fk_out_degree) / 10.0)`
- fill rate: uses `ColumnStats.fill_rate` when available, otherwise falls back
  to `1.0` for non-nullable columns and `0.5` for nullable columns
- indexes: `0.0` with no indexes, `0.3` with only primary indexes, `0.6` with
  at least one non-primary index, `1.0` with two or more non-primary indexes
  and at least one unique index
- name: penalizes staging or temp names
- comment: `1.0` only when a non-empty table comment exists

`score_all(schema=None)`:

- computes `TableScore` for every eligible table
- mutates `table.relevance_score`
- sorts by descending score, then schema and table
- assigns rank starting at `1`

`get_top_tables(n, schema=None)` returns the first `n` ranked scores.

`get_tables_by_domain_cluster()` groups ranked tables by `table.heuristic_type`.

Operational limits:

- fill rate is approximate when connectors do not populate `ColumnStats`
- comment scoring depends entirely on connector comment support
- scoring is structural only and does not include semantic or query workload
  signals in this phase

## Phase 6C anomaly detection contract

`AnomalyDetector.detect(result)` returns sorted `StructuralAnomaly` items with:

- `anomaly_type`
- `severity`
- `schema`
- `table`
- `column`
- `description`
- `suggestion`
- `location`

Severity values:

- `info`
- `warning`
- `critical`

Phase 6C implements these anomaly types:

- `no_indexes`
- `no_pk`
- `high_nullable_no_pk`
- `ambiguous_column_name`
- `fk_without_index`
- `empty_table`
- `implicit_fk`
- `wide_table`

Detection rules:

- views, materialized views, and foreign tables are excluded from `no_indexes`
  and `no_pk`
- staging tables downgrade `no_indexes` from warning to info
- ambiguous names are detected per-column from a fixed vocabulary
- `fk_without_index` validates the source FK columns against the set of indexed
  columns on the same table
- `empty_table` ignores staging/temp tables
- `implicit_fk` is triggered by `_id` columns without declared foreign keys
- `wide_table` uses the fixed threshold of `50` columns

`summarize(anomalies)` returns counts by anomaly type.

Operational limits:

- anomaly detection works only on declared indexes currently exposed by the
  connector
- implicit FK detection is name-based and can report false positives
- `critical` is reserved for future phases; Phase 6C currently emits `info`
  and `warning`
