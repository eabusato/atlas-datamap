# Phase 1 Contracts

## Engine contract

Phase 1 introduces a real `postgresql` connector implemented in
[`atlas/connectors/postgresql.py`](../../atlas/connectors/postgresql.py).

Dependency options:

- runtime extra: `atlas-datamap[postgresql]`
- development extra: `atlas-datamap[dev]`

Driver:

- `psycopg2-binary>=2.9`

## Connection and transport contract

`PostgreSQLConnector.connect()` must:

- create a `ThreadedConnectionPool`
- apply `connect_timeout` from `timeout_seconds`
- map config `ssl_mode="preferred"` to PostgreSQL `sslmode=prefer`
- preserve configured `sslcert`, `sslkey`, and `sslrootcert` for verifying SSL
  modes
- detect and cache the PostgreSQL server version

Each borrowed session must:

- be configured as read-only
- disable autocommit during query execution
- set `statement_timeout` to `timeout_seconds * 1000`
- set `lock_timeout` to `5000` milliseconds
- rollback before returning the connection to the pool

## Schema and table introspection contract

Schema discovery:

- uses `information_schema.schemata`
- excludes `pg_%` schemas
- excludes `information_schema`
- still applies `schema_filter` and `schema_exclude`

Table discovery:

- includes `BASE TABLE`
- includes `VIEW`
- includes `MATERIALIZED VIEW`
- preserves table comments when available

## Column contract

Column introspection must return:

- column name
- ordinal position
- native PostgreSQL type, including array and numeric precision rendering
- nullability
- primary-key flag
- auto-increment detection from sequence or generated defaults
- default value text
- column comment when available

Supported native-type formatting includes:

- `character varying(n)`
- `numeric(p,s)`
- PostgreSQL arrays such as `integer[]`
- `USER-DEFINED` types via `udt_name`

## Relationship contract

Declared foreign keys:

- come from `pg_constraint`
- preserve source column order
- preserve target column order
- expose `on_delete` and `on_update`
- are marked with `is_inferred = false`

Implicit foreign keys:

- are inferred only for columns ending in `_id`
- require canonical type `integer` or `unknown`
- are skipped when the source column already belongs to a declared FK
- target only same-schema tables
- use simple singular/plural heuristics:
  - `<name>`
  - `<name>s`
  - `<name>es`
  - Portuguese `ao -> oes`
- are marked with `is_inferred = true`

## Index contract

Index introspection must expose:

- index name
- source schema and table
- ordered column list
- uniqueness flag
- primary-key flag
- partial-index flag
- access method name such as `btree`

Redundant-index detection:

- considers only non-partial indexes
- marks an index redundant when its ordered column list is a strict prefix of
  another non-partial index

## Statistics and sampling contract

Row-count estimation:

- prefers `pg_stat_user_tables.n_live_tup`
- falls back to `pg_class.reltuples`
- returns `0` when neither source is available

Table size:

- uses `pg_relation_size`

Column statistics:

- use `pg_stats`
- compute `null_count` from `null_frac * row_count`
- compute `distinct_count` from `n_distinct`
- interpret negative `n_distinct` as a fraction of table cardinality
- derive `min_value` and `max_value` from `histogram_bounds`

Sampling:

- is blocked in `stats_only`
- is blocked in `no_samples`
- is allowed in `normal`
- is allowed in `masked`, with sensitive-name masking applied after fetch
- uses plain `LIMIT` when estimated rows are below `10000`
- uses `TABLESAMPLE SYSTEM` when estimated rows are `>= 10000`

## Serialization and orchestration contract

`introspect_schema()` must additionally:

- attach inferred foreign keys after declared foreign-key discovery
- mark source columns referenced by any FK as `is_foreign_key = true`
- mark indexed columns as `is_indexed = true`
- preserve the Phase 0 `SchemaInfo` contract

`introspect_all()` must preserve the Phase 0 `IntrospectionResult` shape and
still compute `fk_in_degree_map`.

## Operational limits

- Only PostgreSQL is fully implemented in this phase.
- Masking remains heuristic and column-name based.
- `pg_stats` values depend on `ANALYZE` freshness.
- `TABLESAMPLE SYSTEM` is approximate and not guaranteed to return an exact row
  count.
- Implicit FK inference does not cross schemas and assumes target primary keys
  named `id`.
