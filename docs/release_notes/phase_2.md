# Phase 2 Release Notes

## Added

- Real MySQL and MariaDB connector backed by `mysql-connector-python`.
- Real SQL Server connector backed by `pyodbc`.
- Docker-backed Phase 2 integration fixtures for MySQL 8, MariaDB 10.6, and
  SQL Server 2022.
- Shared native-type normalization in
  [`type_mapping.py`](../../atlas/connectors/type_mapping.py).
- Expanded `AtlasType` vocabulary with numeric-width, money, XML, spatial, and
  timezone-aware temporal categories.
- Phase 2 unit and integration tests for 2A, 2B, and 2C.

## Verified

- MySQL and MariaDB connection lifecycle, schema discovery, metadata reads,
  FK/index introspection, estimates, and privacy-aware sampling.
- SQL Server connection lifecycle, schema discovery, table/view/synonym
  introspection, comments, FK/index reads, estimates, and privacy-aware
  sampling.
- Canonical type propagation across PostgreSQL, MySQL, MariaDB, and SQL
  Server.
- MariaDB JSON alias normalization from `LONGTEXT CHECK (json_valid(...))`.
- Full historical regression through `tests/run_tests.sh`.

## Notes

- SQL Server integration uses `pyodbc`; local development on macOS also needs
  an installed ODBC driver such as FreeTDS via `unixodbc`.
- `TABLESAMPLE` remains approximate on PostgreSQL and SQL Server, so the
  connectors keep deterministic fallbacks when the sampled result is empty.
