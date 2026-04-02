# Phase 0 Release Notes

## Added

- Installable `atlas-datamap` package metadata and editable-install workflow.
- Canonical repository layout for future roadmap modules.
- Root CLI with working `--help` and `--version`.
- `AtlasConnectionConfig` with URL, TOML, env, dict, and JSON loading.
- Privacy mode and database engine enums.
- Canonical metadata dataclasses with JSON-capable serialization.
- `BaseConnector` orchestration and privacy-aware helpers.
- Minimal SQLite connector for stdlib-backed factory coverage.
- Unit and integration tests for 0A, 0B, and 0C.
- `tests/run_tests.sh` as the single test coordinator for current and future phases.

## Verified

- Editable installation under Python 3.12.
- `atlas --help`, `atlas --version`, and `python -m atlas --help`.
- Build artifact generation for both wheel and sdist.
- Config validation and serialization contracts.
- Metadata serialization round-trips.
- Connector orchestration with a stub implementation and a real SQLite file.

## Notes

- Real PostgreSQL, MySQL, and SQL Server connectors remain out of scope for
  Phase 0.
- Placeholder CLI subcommands are intentionally registered but not yet
  implemented.
