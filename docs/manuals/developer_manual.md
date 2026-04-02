# Developer Manual

## Project context

Atlas Datamap is maintained as its own Python repository at
[`github.com/eabusato/atlas-datamap`](https://github.com/eabusato/atlas-datamap).
Its visual and architectural lineage comes from the CCT project at
[`github.com/eabusato/cct`](https://github.com/eabusato/cct).

The Atlas repository may vendor a constrained subset of CCT sigilo-related
sources for native rendering, but Atlas remains a separate product with its own
tests, packaging, CLI, SDK, and release cycle.

## Environment

Phase 0, Phase 1, and Phase 2 were validated with Python 3.12 in a local virtual
environment:

```bash
/opt/homebrew/bin/python3.12 -m venv .venv312
source .venv312/bin/activate
python -m pip install -e ".[dev]"
```

The repository-local `.venv` present before this phase used Python 3.10 and
does not satisfy the package requirement.

Phase 1 and Phase 2 integration tests also require:

- Docker Desktop running
- a reachable local Docker daemon
- the PostgreSQL, MySQL, and SQL Server drivers from the `dev` extra
- an installed ODBC driver for SQL Server integration; the current macOS
  workflow uses `unixodbc` plus FreeTDS

Phase 13 generic-connector validation also requires:

- `sqlalchemy` installed in the active environment
- Docker access when validating MariaDB differentiation against the real service

Phase 3 native build and sigilo validation also require:

- `make`
- optionally `cmake` for the preferred build path
- a working C toolchain (`cc`, `gcc`, or `clang`)

Phase 14 packaging validation also requires:

- `build`
- `cibuildwheel`
- `pdoc`

## Local workflows

### Lint and typing

```bash
make lint
make typecheck
```

### Phase-oriented tests

For incremental work:

```bash
source .venv312/bin/activate
python -m pytest --no-cov tests/test_version.py tests/integration/phase_0/test_0a_packaging.py
python -m pytest --no-cov tests/test_config.py tests/integration/phase_0/test_0b_config.py
python -m pytest --no-cov tests/test_types.py tests/test_base_connector.py tests/test_connectors_factory.py tests/integration/phase_0/test_0c_connectors.py
python -m pytest --no-cov tests/test_postgresql_1a.py tests/integration/phase_1/test_1a_connection.py
python -m pytest --no-cov tests/test_postgresql_1b.py tests/integration/phase_1/test_1b_tables_columns.py
python -m pytest --no-cov tests/test_postgresql_1c.py tests/integration/phase_1/test_1c_relationships_indexes.py
python -m pytest --no-cov tests/test_postgresql_1d.py tests/integration/phase_1/test_1d_sampling_stats.py
python -m pytest --no-cov tests/test_mysql_2a.py tests/integration/phase_2/test_2a_mysql.py
python -m pytest --no-cov tests/test_mssql_2b.py tests/integration/phase_2/test_2b_mssql.py
python -m pytest --no-cov tests/test_type_mapping.py tests/integration/phase_2/test_2c_type_mapping.py
python -m pytest --no-cov tests/test_c_render.py tests/integration/phase_3/test_3a_c_library.py
python -m pytest --no-cov tests/test_sigilo_3b.py tests/integration/phase_3/test_3b_sigilo.py
python -m pytest --no-cov tests/test_build_3c.py tests/integration/phase_3/test_3c_build_flow.py
python -m pytest --no-cov tests/test_datamap_4a.py tests/integration/phase_4/test_4a_datamap.py
python -m pytest --no-cov tests/test_hover_4b.py tests/integration/phase_4/test_4b_hover.py
python -m pytest --no-cov tests/test_layout_4c.py tests/integration/phase_4/test_4c_layout.py
python -m pytest --no-cov tests/test_scan_5a.py tests/integration/phase_5/test_5a_scan.py
python -m pytest --no-cov tests/test_open_5b.py tests/integration/phase_5/test_5b_open.py
python -m pytest --no-cov tests/test_info_5c.py tests/integration/phase_5/test_5c_info.py
python -m pytest --no-cov tests/test_classifier_6a.py tests/integration/phase_6/test_6a_classifier.py
python -m pytest --no-cov tests/test_scorer_6b.py tests/integration/phase_6/test_6b_scorer.py
python -m pytest --no-cov tests/test_anomalies_6c.py tests/integration/phase_6/test_6c_anomalies.py
python -m pytest --no-cov tests/test_search_7a.py tests/integration/phase_7/test_search_7a.py
python -m pytest --no-cov tests/test_discovery_7b.py tests/integration/phase_7/test_discovery_7b.py
python -m pytest --no-cov tests/test_report_7c.py tests/integration/phase_7/test_report_7c.py
python -m pytest --no-cov tests/integration/phase_8/test_client_8a.py
python -m pytest --no-cov tests/integration/phase_8/test_sampler_8b.py
python -m pytest --no-cov tests/integration/phase_8/test_enricher_8c.py
python -m pytest --no-cov tests/integration/phase_9/test_enricher_9a.py
python -m pytest --no-cov tests/integration/phase_9/test_sigilo_semantic_9b.py
python -m pytest --no-cov tests/integration/phase_9/test_cli_enrich_9c.py
python -m pytest --no-cov tests/integration/phase_10/test_qa_10a.py
python -m pytest --no-cov tests/integration/phase_10/test_vector_10b.py
python -m pytest --no-cov tests/integration/phase_10/test_cli_ask_10c.py
python -m pytest --no-cov tests/integration/phase_11/test_snapshot_11a.py
python -m pytest --no-cov tests/integration/phase_11/test_snapshot_diff_11b.py
python -m pytest --no-cov tests/integration/phase_11/test_cli_history_11c.py
python -m pytest --no-cov tests/integration/phase_12/test_standalone_12a.py
python -m pytest --no-cov tests/integration/phase_12/test_structured_12b.py
python -m pytest --no-cov tests/integration/phase_12/test_report_exec_12c.py
python -m pytest --no-cov tests/integration/phase_13/test_sqlite_13a.py
python -m pytest --no-cov tests/integration/phase_13/test_mariadb_13b.py
python -m pytest --no-cov tests/integration/phase_13/test_generic_13c.py
python -m pytest --no-cov tests/integration/phase_14/test_public_api_14a.py
python -m pytest --no-cov tests/integration/phase_14/test_regression_14b.py --update-baseline
python -m pytest --no-cov tests/integration/phase_14/test_distribution_14c.py
python -m pytest --no-cov tests/test_onboarding.py tests/integration/phase_14/test_onboarding_14d.py
```

### Full block validation

```bash
tests/run_tests.sh
```

The coordinator script:

- ensures editable installation is available
- runs Phase 0A, 0B, 0C, 1A, 1B, 1C, 1D, 2A, 2B, 2C, 3A, 3B, 3C, 4A, 4B, 4C,
  5A, 5B, 5C, 6A, 6B, 6C, 7A, 7B, 7C, 8A, 8B, 8C, 9A, 9B, 9C, 10A, 10B, and
  10C, 11A, 11B, and 11C, 12A, 12B, 12C, 13A, 13B, 13C, 14A, 14B, and 14C
  sequentially
- runs a full Phase 0 regression suite, a combined Phase 0/1 regression suite,
  a combined Phase 0/1/2 regression suite, and a combined Phase 0/1/2/3
  regression suite, plus a combined Phase 0/1/2/3/4 regression suite and a
  combined Phase 0/1/2/3/4/5 regression suite, plus a combined
  Phase 0/1/2/3/4/5/6 regression suite, plus a combined
  Phase 0/1/2/3/4/5/6/7 regression suite, plus a combined
  Phase 0/1/2/3/4/5/6/7/8 regression suite, plus a combined
  Phase 0/1/2/3/4/5/6/7/8/9 regression suite, plus a combined
  Phase 0/1/2/3/4/5/6/7/8/9/10 regression suite, plus a combined
  Phase 0/1/2/3/4/5/6/7/8/9/10/11 regression suite, plus a combined
  Phase 0/1/2/3/4/5/6/7/8/9/10/11/12/13 regression suite, plus a combined
  Phase 0/1/2/3/4/5/6/7/8/9/10/11/12/13/14 regression suite
- emits explicit status messages showing that historical suites stayed green

### CLI-focused validation

```bash
make test-cli
```

This target runs the Phase 5 command suites:

- `tests/test_scan_5a.py`
- `tests/test_open_5b.py`
- `tests/test_info_5c.py`
- `tests/integration/phase_5/`

### Native sigilo workflows

```bash
make build-c
make test-c-smoke
make test-sigilo
python scripts/check_sigilo_build.py
```

`make build-c` uses [`atlas/_c/build_lib.py`](../../atlas/_c/build_lib.py)
to prefer CMake and fall back to the vendored Makefile when CMake is not
available. `setup.py build_ext --inplace` reuses the same helper.

## Test artifact policy

- All generated artifacts stay under `tests/tmp/`.
- No test in this phase uses `/tmp`.
- Integration tests are grouped under `tests/integration/phase_0/`.
- Phase 1 PostgreSQL integration data is provisioned through
  `tests/integration/docker-compose.yml` and session fixtures in
  `tests/integration/conftest.py`.
- Phase 2 reuses the same Docker Compose file for MySQL, MariaDB, and SQL
  Server fixtures and keeps all transient artifacts inside `tests/tmp/`.
- Phase 3 keeps native-build scratch directories and smoke artifacts under
  `tests/tmp/` as well.
- Phase 4 keeps all phase-local integration suites under
  `tests/integration/phase_4/` and continues to avoid `/tmp`.
- Phase 5 keeps all phase-local integration suites under
  `tests/integration/phase_5/` and continues to avoid `/tmp`.
- Phase 6 keeps all phase-local integration suites under
  `tests/integration/phase_6/` and continues to avoid `/tmp`.
- Phase 7 keeps all phase-local integration suites under
  `tests/integration/phase_7/` and continues to avoid `/tmp`.
- Phase 8 keeps all phase-local integration suites under
  `tests/integration/phase_8/` and continues to avoid `/tmp`.
- Phase 9 keeps all phase-local integration suites under
  `tests/integration/phase_9/` and continues to avoid `/tmp`.
- Phase 10 keeps all phase-local integration suites under
  `tests/integration/phase_10/` and continues to avoid `/tmp`.
- Phase 11 keeps all phase-local integration suites under
  `tests/integration/phase_11/` and continues to avoid `/tmp`.
- Phase 12 keeps all phase-local integration suites under
  `tests/integration/phase_12/` and continues to avoid `/tmp`.
- Phase 13 keeps all phase-local integration suites under
  `tests/integration/phase_13/` and continues to avoid `/tmp`.
- Phase 14 keeps all phase-local integration suites under
  `tests/integration/phase_14/`, keeps the approved SVG baselines in
  `tests/baselines/phase_14/`, and continues to avoid `/tmp`.
- The onboarding flow writes its local user workspace outside `tests/tmp/` in
  production, but integration tests still keep all generated onboarding
  artifacts under pytest-managed temporary directories.

## Future-extension policy

- Add new phase suites to `tests/run_tests.sh` instead of creating parallel test
  runners.
- Keep user-facing strings and variable names in English.
- Preserve Phase 0 serialization formats when adding downstream consumers.
- Keep the vendored CCT files synchronized through explicit file copies and
  version headers instead of ad hoc edits inside `atlas/_c/common/` and
  `atlas/_c/sigil/`.
- Preserve the Phase 4 SVG wrapper classes (`system-node-wrap`,
  `system-edge-wrap`) and `data-*` attributes because the hover script and
  downstream consumers depend on them.
- Preserve the Phase 5 artifact trio (`.svg`, `.sigil`, `_meta.json`) and the
  `atlas info` selective-fetch behavior when evolving the CLI.
- Preserve the Phase 6 in-place metadata mutation contract:
  `heuristic_type`, `heuristic_confidence`, and `relevance_score` are written
  back to `TableInfo` rather than returned only as side-channel output.
- Preserve the current sigilo visual contract when evolving search and
  reporting. New Phase 7 features may embed or reference sigilo output, but
  they must not silently regress the existing rendering style.
- Preserve the Phase 8 semantic-firewall contract: prompt context must continue
  to redact structured PII tags and must not bypass `PrivacyMode`.
- Preserve the Phase 8 in-place semantic mutation contract: semantic metadata
  is written back into `TableInfo` and `ColumnInfo` rather than returned only
  as detached side-channel structures.
- Preserve the Phase 9 semantic cache signature contract: structural changes to
  tables and columns must invalidate cache reuse.
- Preserve the Phase 9 sigilo semantic surface: `data-semantic-*` attributes
  and instant hover semantics may evolve, but they must not silently regress
  the approved visual formulation of the sigilo.
- Preserve the Phase 10 `atlas ask` JSON contract: `--format json` must remain
  clean on `stdout` without mixed operational logs.
- Preserve the Phase 10 vector sidecar contract: embeddings persist beside the
  `.sigil` in `<stem>.embeddings`, not inside the `.sigil` payload itself.
- Preserve the Phase 11 archive contract: `.atlas` remains ZIP-backed, keeps
  `schema.json` and `sigilo.sigil` as separate members, and remains readable
  offline without reconnecting to the source database.
- Preserve the Phase 11 history contract: `latest` resolution and `YYYYMMDD`
  date references must stay deterministic and must not silently choose an
  ambiguous snapshot.
- Preserve the Phase 12 standalone-export contract: offline HTML wrappers may
  add navigation and detail panels, but they must not rewrite the approved
  sigilo visual formulation.
- Preserve the Phase 12 structured-export contract: JSON/CSV/Markdown outputs
  must continue to use English headers and the real `tables` / `columns`
  semantic snapshot shape.
- Preserve the Phase 12 reporting contract: `atlas report --style health`
  remains backward-compatible while `--style executive` stays fully offline and
  may not depend on external assets.
- Preserve the Phase 13 generic URL contract: `generic+<dialect>://...` must
  continue to preserve the real SQLAlchemy DSN in `connect_args["sqlalchemy_url"]`.
- Preserve the Phase 13 MariaDB contract: MariaDB remains a variant of the
  MySQL connector path rather than a separate `DatabaseEngine`.
- Preserve the Phase 14 public SDK contract: `Atlas` is a facade over existing
  modules, not a second internal implementation.
- Preserve the Phase 14 SVG baseline contract: normalization may remove only
  non-visual volatility and must not hide structural sigilo regressions.

## Distribution workflows

```bash
source .venv312/bin/activate
python -m build
python -m cibuildwheel --output-dir dist/wheels
python -m pip install --force-reinstall --no-deps dist/atlas_datamap-*.whl
python -m atlas --help
```

See [`publishing.md`](../publishing.md) for the
full release checklist and the tag-driven publish workflow.
