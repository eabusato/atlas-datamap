# Phase 13 Contracts

## SQLite connector contract

Phase 13A hardens the built-in SQLite support through
[`atlas/connectors/sqlite.py`](../../atlas/connectors/sqlite.py).

Runtime rules:

- `SQLiteConnector` remains the factory target for `DatabaseEngine.sqlite`
- schema discovery is fixed to `main`
- tables and views come from `sqlite_master`
- columns, foreign keys, and indexes come from `PRAGMA`
- row estimates use `COUNT(*)`
- table-level `size_bytes` remains `0`
- schema-level `total_size_bytes` is populated from the `.db` file size
- sampling uses `LIMIT` and still honors `PrivacyMode`

SQLite-specific contracts:

- composite foreign keys are grouped by `PRAGMA foreign_key_list().id`
- internal indexes named `sqlite_%` are ignored only when they are plain
  catalog artifacts without analytical value
- type affinity is normalized into `AtlasType` without changing the cross-engine
  canonical vocabulary

## MariaDB differentiation contract

Phase 13B keeps MariaDB on the MySQL connector path instead of introducing a
new `DatabaseEngine`.

Configuration rules:

- `mysql://...` and `mariadb://...` both map to `DatabaseEngine.mysql`
- `get_connector()` still returns `MySQLConnector`
- server-version detection decides whether the live target is MariaDB

Result-shape rules when `is_mariadb` is true:

- `IntrospectionResult.engine == "mariadb"`
- each `SchemaInfo.engine == "mariadb"`
- each schema may carry `extra_metadata`

MariaDB-specific `extra_metadata` keys:

- `mariadb_routines`: list of dicts with `name`, `type`, and `comment`
- `mariadb_sequences`: list of sequence names when the engine exposes them

Operational limit:

- some MariaDB builds used by the integration suite do not expose
  `information_schema.SEQUENCES`
- the connector therefore degrades gracefully:
  - first tries `information_schema.SEQUENCES`
  - then falls back to `information_schema.TABLES` with `TABLE_TYPE='SEQUENCE'`
  - returns `[]` if neither path is available

Type-normalization additions for `engine="mariadb"`:

- `INET4` -> `AtlasType.TEXT`
- `INET6` -> `AtlasType.TEXT`
- `UUID` -> `AtlasType.UUID`
- `JSON` stays `AtlasType.JSON`

## Generic SQLAlchemy connector contract

Phase 13C adds a degraded connector through
[`atlas/connectors/generic.py`](../../atlas/connectors/generic.py).

Entry contract:

- generic URLs use `generic+<dialect>://...`
- `AtlasConnectionConfig.from_url(...)` stores the real SQLAlchemy DSN in
  `connect_args["sqlalchemy_url"]`
- `connection_string_safe` preserves the `generic+<dialect>` identity while
  stripping the password

Factory rule:

- `DatabaseEngine.generic` resolves to `SQLAlchemyConnector`

Introspection rules:

- uses SQLAlchemy `create_engine(...)` and `inspect(...)`
- supports schemas, tables, views, columns, PKs, FKs, and indexes on a
  best-effort basis
- `IntrospectionResult.engine == "generic"`
- `SchemaInfo.engine == "generic"`

Degraded metrics contract:

- `row_count_estimate = 0`
- `size_bytes = 0`
- `get_column_null_count(...) = 0`
- `get_column_distinct_estimate(...) = 0`

Sampling contract:

- sampling is best-effort only
- when the dialect can execute a simple limited `SELECT`, rows are returned
- when the dialect or SQL translation fails, the connector returns `[]`
  instead of crashing
- privacy masking still applies to the returned rows

Dependency contract:

- SQLAlchemy is optional
- the package imports without SQLAlchemy installed
- using the generic connector without SQLAlchemy raises a clear English
  installation error

## Test runner contract

Phase 13 extends the unified coordinator in
[`tests/run_tests.sh`](../../tests/run_tests.sh):

- `13A` runs `tests/integration/phase_13/test_sqlite_13a.py`
- `13B` runs `tests/integration/phase_13/test_mariadb_13b.py`
- `13C` runs `tests/integration/phase_13/test_generic_13c.py`
- `ALL` now executes the full regression suite through Phase 13

The runner also ensures the dev environment contains SQLAlchemy before running
the generic-connector phase.
