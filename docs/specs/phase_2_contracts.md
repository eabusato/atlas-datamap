# Phase 2 Contracts

## Engine contract

Phase 2 introduces real `mysql` and `mssql` connectors and extends all real
connectors with cross-engine canonical type normalization.

Implemented modules:

- [`atlas/connectors/mysql.py`](../../atlas/connectors/mysql.py)
- [`atlas/connectors/mssql.py`](../../atlas/connectors/mssql.py)
- [`atlas/connectors/type_mapping.py`](../../atlas/connectors/type_mapping.py)

Dependency options:

- runtime extra: `atlas-datamap[mysql]`
- runtime extra: `atlas-datamap[mssql]`
- development extra: `atlas-datamap[dev]`

Drivers:

- `mysql-connector-python>=8.3`
- `pyodbc>=5.1`

## MySQL and MariaDB transport contract

`MySQLConnector.connect()` must:

- create a `MySQLConnectionPool`
- apply `connection_timeout` from `timeout_seconds`
- detect whether the target server is MySQL or MariaDB
- cache the server version tuple

Each borrowed session must:

- disable writes through a read-only transaction when the engine supports it
- set a statement timeout through `max_execution_time` or
  `max_statement_time`
- rollback before returning the connection to the pool

Schema and metadata discovery must:

- use `information_schema`
- exclude `information_schema`, `mysql`, `performance_schema`, and `sys`
- return tables and views with comments when available
- return columns, defaults, PK/unique flags, FKs, indexes, estimated row
  counts, and table sizes

Sampling must:

- respect all Phase 0 privacy modes
- use `ORDER BY RAND()` on smaller tables
- use `LIMIT/OFFSET` random sampling on larger tables
- fall back to plain `LIMIT` when the randomized large-table sample is empty

## SQL Server transport contract

`MSSQLConnector.connect()` must:

- create a queue-backed connection pool
- build a `pyodbc` connection string from `AtlasConnectionConfig`
- support explicit driver overrides through `connect_args["driver"]`
- cache the server version tuple and edition

Each borrowed session must:

- set `READ UNCOMMITTED`
- set `LOCK_TIMEOUT 5000`
- rollback before returning the connection to the pool

Schema and metadata discovery must:

- use SQL Server system catalogs
- exclude SQL Server system schemas
- return tables, views, and synonyms
- preserve table and column comments from `MS_Description`
- return columns, defaults, PK flags, identity flags, FKs, indexes, estimated
  row counts, and table sizes

Sampling must:

- respect all Phase 0 privacy modes
- use randomized `TOP` selection on smaller tables
- use `TABLESAMPLE` on larger tables
- fall back to randomized `TOP` sampling if `TABLESAMPLE` returns no rows

## Canonical type contract

From Phase 2C onward, all real connectors must populate
`ColumnInfo.canonical_type` explicitly in `get_columns()`.

The canonical mapping lives in
[`atlas/connectors/type_mapping.py`](../../atlas/connectors/type_mapping.py)
and must:

- never raise exceptions
- accept `postgresql`, `mysql`, `mariadb`, `mssql`, and `sqlserver`
- preserve the native type string separately in `ColumnInfo.native_type`
- distinguish:
  - `INTEGER`, `SMALLINT`, `BIGINT`, `TINYINT`
  - `FLOAT`, `DOUBLE`, `DECIMAL`, `MONEY`
  - `TEXT`, `CHAR`, `CLOB`
  - `DATETIME`, `TIMESTAMP`, `DATE`, `TIME`, `INTERVAL`
  - `BINARY`, `JSON`, `XML`, `ARRAY`, `ENUM`, `UUID`, `SPATIAL`

Special cases implemented in this phase:

- MySQL and MariaDB `tinyint(1)` and `bit(1)` map to `BOOLEAN`
- PostgreSQL `timestamp with time zone` maps to `TIMESTAMP`
- PostgreSQL array suffixes map to `ARRAY`
- PostgreSQL `USER-DEFINED` maps to `UNKNOWN`
- SQL Server `timestamp` and `rowversion` map to `BINARY`
- MariaDB JSON aliases declared as `LONGTEXT CHECK (json_valid(column))`
  map to `JSON`

## Operational limits

- SQL Server integration currently assumes a working ODBC driver, and the
  current development flow uses FreeTDS on macOS.
- MariaDB JSON detection depends on the standard `json_valid(column)` alias
  pattern exposed by the catalog.
- Cross-engine type normalization is limited to catalog-visible native type
  strings and explicit special cases; it does not infer semantic domain types
  from business naming.
