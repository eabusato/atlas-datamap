# Phase 13 Release Notes

## Scope

Phase 13 expands connector coverage with a hardened SQLite connector, explicit
MariaDB differentiation on top of the MySQL path, and a degraded generic
connector powered by SQLAlchemy.

## Delivered

- Hardened
  [`atlas/connectors/sqlite.py`](../../atlas/connectors/sqlite.py)
  with:
  - file-backed read-only connections when possible
  - `sqlite_master` table/view discovery
  - grouped composite foreign keys from `PRAGMA foreign_key_list`
  - explicit index extraction from `PRAGMA index_list` and `PRAGMA index_info`
  - real row counts and column stats
  - schema-level size accounting from the `.db` file size
- Extended
  [`atlas/types.py`](../../atlas/types.py)
  so `SchemaInfo` can carry serializable `extra_metadata`
- Extended
  [`atlas/connectors/mysql.py`](../../atlas/connectors/mysql.py)
  to:
  - mark results as `mariadb` when the server variant is MariaDB
  - capture `ROUTINES`
  - capture sequences when the catalog exposes them, with graceful fallback
- Extended
  [`atlas/connectors/type_mapping.py`](../../atlas/connectors/type_mapping.py)
  with MariaDB-specific normalization for `INET4`, `INET6`, and `UUID`
- Added the degraded generic connector in
  [`atlas/connectors/generic.py`](../../atlas/connectors/generic.py)
- Extended
  [`atlas/config.py`](../../atlas/config.py)
  to parse `generic+<dialect>://...` URLs and preserve the real SQLAlchemy DSN
- Extended
  [`atlas/connectors/__init__.py`](../../atlas/connectors/__init__.py)
  so the factory resolves `DatabaseEngine.generic`
- Added integration coverage in
  [`tests/integration/phase_13/`](../../tests/integration/phase_13)
- Extended the unified runner and pytest markers for Phase 13

## Notes

- Phase 13 does not touch the sigilo renderers or the approved visual
  formulation.
- MariaDB sequence extraction degrades to an empty list on builds where the
  catalog does not expose sequences through `information_schema`.
- The generic connector intentionally returns degraded metrics (`0`) instead of
  fabricating physical statistics.
