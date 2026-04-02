# Phase 0 Contracts

## Packaging contract

- Package name: `atlas-datamap`
- Minimum Python version: `3.11`
- Console entrypoint: `atlas = atlas.cli:cli`
- Module entrypoint: `python -m atlas`

## Parsing contract

### URL parsing

`AtlasConnectionConfig.from_url()` supports:

- `postgresql://`
- `postgres://`
- `mysql://`
- `mariadb://`
- `mssql://`
- `sqlserver://`
- `sqlite://`
- `generic://`

Rules:

- non-SQLite URLs require `host`
- `database` is always required
- query parameters are copied into `connect_args`
- percent-encoded usernames and passwords are decoded

### TOML parsing

Expected sections:

- `[connection]`
- `[connection.connect_args]`
- `[analysis]`

Operational limits:

- missing `[connection].engine` is an error
- invalid `privacy_mode` or `engine` values are rejected
- absent values fall back to Phase 0 defaults

### Environment parsing

Supported variables:

- `ATLAS_ENGINE`
- `ATLAS_HOST`
- `ATLAS_PORT`
- `ATLAS_DATABASE`
- `ATLAS_USER`
- `ATLAS_PASSWORD`
- `ATLAS_SSL_MODE`
- `ATLAS_TIMEOUT`
- `ATLAS_SAMPLE_LIMIT`
- `ATLAS_PRIVACY_MODE`
- `ATLAS_SCHEMA_FILTER`
- `ATLAS_SCHEMA_EXCLUDE`

Comma-separated schema lists are trimmed and normalized into Python lists.

## Validation contract

- `timeout_seconds` must be `>= 1`
- `sample_limit` must be between `1` and `10000`
- `port`, when present, must be between `1` and `65535`
- `ssl_mode` must be one of:
  - `disable`
  - `require`
  - `verify-ca`
  - `verify-full`
  - `preferred`

## Serialization contract

### `AtlasConnectionConfig`

- `to_dict()` masks `password` by default
- `to_dict(include_password=True)` emits the raw password
- `to_json()` and `from_json()` round-trip the config

### Canonical metadata types

Every metadata type supports `to_dict()` and `from_dict()`.
`IntrospectionResult` additionally supports `to_json()` and `from_json()`.

## Privacy contract

- `normal`: row samples allowed, raw values allowed
- `masked`: row samples allowed, sensitive columns masked
- `stats_only`: row samples blocked
- `no_samples`: row samples blocked

## Connector orchestration contract

`BaseConnector.introspect_schema()` must populate:

- columns
- foreign keys
- indexes
- `column_count`
- `row_count_estimate`
- `size_bytes`

`BaseConnector.introspect_all()` must:

- introspect every included schema
- compute `fk_in_degree_map`
- populate incoming FK degree on each table object
