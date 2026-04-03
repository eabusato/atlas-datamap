# Atlas Architecture

Atlas Datamap grew out of the sigilo and systems-thinking work behind CCT, but
it is shipped as its own standalone Python product. The phase names used in the
repository describe the historical implementation order; the architecture below
describes the current system as a whole.

1. Packaging and installation through `pyproject.toml`, `setup.py`, and a
   canonical `Makefile`.
2. A single connection configuration model in [`atlas/config.py`](../atlas/config.py).
3. Canonical metadata types in [`atlas/types.py`](../atlas/types.py).
4. A connector abstraction in [`atlas/connectors/base.py`](../atlas/connectors/base.py).
5. Real connector implementations for PostgreSQL, MySQL/MariaDB, SQL Server,
   SQLite, and generic SQLAlchemy adapters.

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

- Future work may add new modules, fields, and engines.
- Existing public fields and enum values introduced in the early bootstrap
  layers should remain backward compatible.
- Privacy behavior must remain explicit and never depend on mutable global
  state.
- Metadata serialization formats must remain dict/JSON based and stable enough
  for downstream phases to consume.
