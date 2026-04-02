# Atlas Architecture

Phase 0 established the bootstrap contracts that every later Atlas phase must
preserve. Phase 1 adds the first real external-engine implementation through
the PostgreSQL connector.

1. Packaging and installation through `pyproject.toml`, `setup.py`, and a
   canonical `Makefile`.
2. A single connection configuration model in [`atlas/config.py`](../atlas/config.py).
3. Canonical metadata types in [`atlas/types.py`](../atlas/types.py).
4. A connector abstraction in [`atlas/connectors/base.py`](../atlas/connectors/base.py).
5. A PostgreSQL catalog adapter in [`atlas/connectors/postgresql.py`](../atlas/connectors/postgresql.py).

## Layers

1. Packaging layer: build metadata, CLI entrypoints, and editable installs.
2. Configuration layer: engine, credentials, schema scope, sampling policy, and
   privacy mode.
3. Metadata layer: schemas, tables, columns, foreign keys, indexes, and full
   introspection results.
4. Connector layer: lifecycle, sampling guards, stats helpers, and orchestration
   for `introspect_schema()` and `introspect_all()`.
5. PostgreSQL catalog layer: pool-backed sessions, `information_schema`,
   `pg_constraint`, `pg_index`, `pg_stats`, and privacy-aware sampling.

## Extension policy

- Future phases may add new modules, fields, and engines.
- Existing public fields and enum values introduced in Phase 0 should remain
  backward compatible.
- Privacy behavior must remain explicit and never depend on mutable global
  state.
- Metadata serialization formats must remain dict/JSON based and stable enough
  for downstream phases to consume.
