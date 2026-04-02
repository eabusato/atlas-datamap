# Phase 1 Release Notes

## Added

- Real PostgreSQL connector backed by `psycopg2`.
- Connection pooling, version detection, SSL argument mapping, and read-only
  session setup.
- Schema discovery excluding PostgreSQL system schemas.
- Introspection for tables, views, materialized views, columns, comments,
  primary keys, defaults, and PostgreSQL native types.
- Declared foreign-key discovery plus same-schema implicit FK inference for
  `<name>_id` conventions.
- Index discovery with uniqueness, primary, partial, and access-method
  metadata.
- Row-count estimation, relation size collection, and `pg_stats`-based column
  statistics.
- Privacy-aware row sampling with `LIMIT` and `TABLESAMPLE SYSTEM`.
- Phase 1 unit and Docker-backed integration tests for 1A, 1B, 1C, and 1D.

## Verified

- PostgreSQL 15 integration tests using Docker Desktop and `docker compose`.
- Connector lifecycle, reconnect behavior, and version parsing.
- Table and column metadata extraction from a seeded PostgreSQL schema.
- Explicit and inferred relationships plus redundant-index detection.
- Sampling behavior under `normal`, `masked`, `stats_only`, and `no_samples`.
- Full historical regression through `tests/run_tests.sh`.

## Notes

- MySQL and SQL Server remain out of scope for real connector delivery in this
  phase.
- Privacy masking still depends on column-name heuristics rather than explicit
  sensitivity classification.
