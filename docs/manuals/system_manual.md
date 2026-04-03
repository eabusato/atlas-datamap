# System Manual

## Purpose

Atlas Datamap is a Python package that models databases as navigable metadata.
The product direction comes from the sigilo concepts first explored in the CCT
project, published at
[`github.com/eabusato/cct`](https://github.com/eabusato/cct). Atlas Datamap
itself is published separately at
[`github.com/eabusato/atlas-datamap`](https://github.com/eabusato/atlas-datamap)
and implements that database-mapping workflow as a standalone Python system.

## Recommended first command

For most operators and evaluators, the correct entrypoint is:

```bash
atlas onboard
```

`atlas onboard` is the guided local-only workflow that collects the database
connection, privacy mode, sigilo preferences, optional local-AI settings, and
optional `.env` handling. It then builds the full Atlas workspace with scan
artifacts, reports, exports, history, and diffing.

The rest of this manual documents the lower-level contracts behind that flow.

For a complete prompt-by-prompt reference, see
[`onboarding_manual.md`](onboarding_manual.md).

The phase references in this manual are historical. They explain how the
current surface was assembled, but the supported product today is the combined
result of all those layers plus the newer onboarding flow.

Phase 0 established the package, configuration, privacy, and metadata
contracts. Phase 1 adds a real PostgreSQL connector with catalog-driven
introspection, statistics, and privacy-aware row sampling. Phase 2 adds real
MySQL/MariaDB and SQL Server connectors plus cross-engine canonical type
normalization. Phase 3 adds native and fallback sigilo rendering from
`IntrospectionResult`. Phase 4 turns sigilo into a database datamap with
style presets, schema-aware grouping, embedded hover metadata, and native
force-directed layout. Phase 5 turns the CLI into a working product surface for
scan, local viewing, and selective table inspection. Phase 6 adds deterministic
analysis heuristics for classification, ranking, and anomaly detection over the
introspected metadata. Phase 7 adds textual search, heuristic discovery, and a
stand-alone HTML health report. Phase 8 adds local LLM integration, a
privacy-preserving prompt-context firewall, and semantic enrichment of table
and column metadata. Phase 9 adds persistent semantic caching, schema-wide
semantic orchestration, sigilo semantic hover payloads, and the `atlas enrich`
workflow. Phase 10 adds hybrid natural-language QA, optional local embeddings,
vector-search sidecars, and the `atlas ask` workflow. Phase 11 adds portable
`.atlas` snapshots, offline snapshot diffing, and local snapshot history.
Phase 12 adds offline standalone sigilo export, structured JSON/CSV/Markdown
exports, and an executive HTML reporting mode. Phase 13 adds a hardened
SQLite connector, explicit MariaDB differentiation on top of the MySQL path,
and a degraded generic SQLAlchemy connector for non-native dialects. Phase 14
adds the stable public Python SDK, sigilo regression baselines, and release-ready
distribution packaging for Step 1. The current post-Phase-14 surface also adds
`atlas onboard`, a guided local-only workflow that builds a full Atlas workspace
for a real user database.

## Runtime surface

### CLI

- `atlas --help`
- `atlas --version`
- `python -m atlas --help`

### Public SDK

Phase 14 adds the stable programmatic facade:

- `from atlas import Atlas, AtlasConnectionConfig`

Canonical flow:

```python
from atlas import Atlas, AtlasConnectionConfig

atlas = Atlas(AtlasConnectionConfig.from_url("sqlite:////absolute/path.db"))
result = atlas.scan()
sigilo = atlas.build_sigilo(result, style="compact")
snapshot = atlas.create_snapshot(result, sigilo)
```

The public facade preserves the contracts of the underlying subsystems instead
of introducing parallel data structures.

The root CLI is functional. The subcommands `scan`, `open`, `info`, `search`,
`report`, `onboard`, `export`, `enrich`, `ask`, `diff`, and `history` are
registered and documented in help output.

`atlas onboard` has its own detailed operator guide in
[`onboarding_manual.md`](onboarding_manual.md).

Current CLI implementation status:

- `scan`: fully implemented in Phase 5A
- `open`: fully implemented in Phase 5B
- `info`: fully implemented in Phase 5C
- `search`: fully implemented in Phase 7A
- `report`: fully implemented in Phase 7C
- `onboard`: guided full-workspace orchestration implemented after Phase 14
- `export`: fully implemented in Phase 12B, with SVG wrapping added in Phase 12A
- `enrich`: fully implemented in Phase 9C
- `ask`: fully implemented in Phase 10C
- `diff`: fully implemented in Phase 11B
- `history`: fully implemented in Phase 11C

Phase 5 command behavior:

- `atlas scan` introspects a database and writes `{db}.svg`, `{db}.sigil`, and
  `{db}_meta.json`
- `atlas open` wraps a rendered SVG in HTML and serves it from a local HTTP
  server to avoid browser `file://` restrictions
- `atlas info` fetches metadata for a single table without scanning the whole
  database
- `atlas search` performs deterministic text search over schemas, tables, and
  columns
- `atlas report` emits a stand-alone HTML health report from a live database or
  from a saved `.sigil` snapshot, and now also supports `.atlas` plus an
  executive style
- `atlas onboard` asks for local connection, privacy, sigilo, and optional AI
  settings, stores only local config and env references, and runs the complete
  Atlas round with reports, exports, history, and diffing
- `atlas export` writes offline standalone HTML wrappers, JSON, CSV, and
  Markdown from `.sigil` or `.atlas` inputs
- `atlas enrich` enriches a saved `.sigil` or a live database with semantic
  metadata, rebuilds the sigilo, and writes enriched artifacts
- `atlas ask` answers natural-language questions against a `.sigil` snapshot or
  a live database, with optional vector candidates when embeddings are
  available
- `atlas diff` compares two `.atlas` snapshots offline and writes an HTML
  change report
- `atlas history` lists, resolves, diffs, and opens local snapshot archives

### Configuration contract

`AtlasConnectionConfig` accepts input from:

- connection URL
- TOML file
- environment variables
- Python dict
- JSON string

Supported engines in the package:

- `postgresql`
- `mysql`
- `mssql`
- `sqlite`
- `generic`

Current real connector coverage:

- `postgresql`: full Phase 1 implementation
- `mysql`: full Phase 2 implementation, including MariaDB compatibility
- `mssql`: full Phase 2 implementation through `pyodbc`
- `sqlite`: initial stdlib-backed implementation introduced in Phase 0
- `sqlite`: hardened in Phase 13 with full PRAGMA-driven metadata and file-size accounting
- `generic`: Phase 13 degraded SQLAlchemy fallback for non-native dialects

Defaults and validation are applied at construction time. Invalid
configurations fail early with `ConfigValidationError`.

Phase 13 generic-connector configuration adds a second URL grammar:

- `generic+<dialect>://...`

Examples:

- `generic+sqlite:////absolute/path.db`
- `generic+cockroachdb://user:pass@host:26257/dbname`

The real SQLAlchemy DSN is preserved in `connect_args["sqlalchemy_url"]`.

### Privacy contract

Every connector receives an explicit `PrivacyMode`. Sampling helpers refuse to
return rows in `stats_only` or `no_samples`. In `masked`, column names that
match the sensitive-name list are redacted to `***`.

Although the first concrete implementation appeared in the PostgreSQL work, the
privacy contract now applies to the shared connector model across Atlas.

The onboarding flow adds one stronger guarantee on top of that privacy model:

- secrets stay in a local `.env` file or an existing env file chosen by the user
- the onboarding manifest stores only env-var references, not resolved secrets
- AI endpoints are restricted to local hosts so semantic prompts stay on the
  user's machine

Practical interpretation of the current privacy contract:

- `masked` is helpful, but it is still based on sensitive column names rather
  than full semantic classification
- metadata-rich artifacts written by Atlas can still be sensitive even when
  they do not contain raw table dumps
- `stats_only` and `no_samples` are the preferred modes when the operator wants
  to avoid sample-derived prompt context
- manual AI configuration outside `atlas onboard` extends the trust boundary to
  the endpoint explicitly chosen by the operator

In Phase 1 PostgreSQL:

- masking remains name-based, not classification-based
- aggregate statistics remain available in every privacy mode
- sample queries run only after privacy checks succeed

### Metadata contract

Canonical types:

- `SchemaInfo`
- `TableInfo`
- `ColumnInfo`
- `ForeignKeyInfo`
- `IndexInfo`
- `ColumnStats`
- `IntrospectionResult`

Each type supports `to_dict()` and `from_dict()`. `IntrospectionResult` and
`AtlasConnectionConfig` also support JSON round-trips.

From Phase 2C onward, every `ColumnInfo` returned by the real connectors
includes:

- `native_type`: the engine-native type string
- `canonical_type`: a normalized `AtlasType` value used for cross-engine
  comparisons

The canonical vocabulary distinguishes integer widths, floating-point families,
fixed-point values, text vs CLOB, timezone-aware timestamps, XML, spatial
types, and money types.

Phase 13 extends metadata at schema scope with optional `SchemaInfo.extra_metadata`
for connector-specific payloads. The first concrete use is MariaDB-specific
schema enrichment:

- `mariadb_routines`
- `mariadb_sequences`

### Sigilo rendering contract

Phase 4 extends `atlas.sigilo` and exports:

- `SigiloBuilder`
- `DatamapSigiloBuilder`
- `SigiloStyle`
- `SigiloConfig`
- `SigiloNode`
- `SigiloEdge`
- `SigiloColumnDesc`

`SigiloBuilder` remains the generic node/edge renderer. `DatamapSigiloBuilder`
is the database-aware entry point that converts `IntrospectionResult` into a
schema-aware SVG datamap. The rendering path is:

1. try `atlas._sigilo` and the vendored `libatlas_sigilo`
2. if the shared library is missing or fails at runtime, fall back to
   `atlas.sigilo._python_fallback`

Both renderers emit SVG with the same operational metadata surface:

- node-level `data-table`, `data-schema`, `data-row-estimate`,
  `data-size-bytes`, `data-column-count`, `data-fk-count`,
  `data-index-count`, `data-table-type`, and `data-comment`
- edge-level `data-fk-from`, `data-fk-to`, `data-fk-columns`, and
  `data-fk-type`, plus `data-on-delete` when available
- `<title>` elements when enabled through `SigiloConfig.emit_titles`
- an embedded `<script>` block that implements immediate hover tooltips for
  `.system-node-wrap` and `.system-edge-wrap`

Phase 4 layout and style behavior:

- styles: `network`, `seal`, `compact`
- layout modes: `circular` and `force`
- graphs with five or fewer nodes always use the circular layout even when
  `force` is requested
- multi-schema renderings emit one macro-ring per schema unless the active
  style disables rings
- views render as `node-aux`, materialized views as `node-loop`, foreign tables
  as `node-fk`
- primary-key columns render as `col-pk`; declared foreign keys as `call`;
  inferred foreign keys as `branch`

The native renderer now supports both the circular schema layout and a
force-directed layout implemented in C. The Python fallback preserves the same
SVG contract and uses a deterministic relaxation step for `force` requests when
the shared library is unavailable.

Phase 14 adds a regression layer over this contract:

- approved SVG baselines live under `tests/baselines/phase_14/`
- normalization removes only non-visual volatility such as build comments,
  ephemeral tooltip ids, and absolute local `.db` paths
- the approved visual formulation of the sigilo is now treated as a release
  contract, not just a best-effort renderer output

### Phase 5 orchestration and artifact contract

Phase 5 adds `IntrospectionRunner` as the orchestration layer used by the CLI.
It owns connector lifecycle, emits progress stages, applies schema filters, and
produces `IntrospectionResult` with a populated `fk_in_degree_map`.

Artifact persistence now follows a fixed contract:

- `.svg`: rendered datamap
- `.sigil`: compact JSON serialization of the introspection result
- `_meta.json`: pretty JSON serialization of the same result

Phase 12 extends downstream export surfaces without changing these base
artifacts:

- `atlas export svg` wraps one sigilo into a single offline HTML document
- `atlas export json` emits structural metadata plus optional `semantic_data`
- `atlas export csv --entity tables|columns` emits stable English inventories
- `atlas export markdown` emits a schema-oriented data dictionary

For SQLite and other path-shaped database names, the CLI uses the basename of
the database path when computing output filenames.

Phase 13 leaves artifact formats unchanged. The new connectors still emit the
same `IntrospectionResult` / `.sigil` / `.atlas` contracts consumed by the rest
of the system.

### Phase 5 local viewer contract

The local viewer depends on:

- inline SVG output from Phase 4
- `data-schema`, `data-table`, and related node attributes
- an HTML wrapper generated fully in memory

The side panel groups nodes by schema, supports live filtering, and highlights
the selected SVG node. The HTTP layer is intentionally minimal and stateless.

Phase 12A adds an offline wrapper that preserves the same SVG contract while
moving the interaction model into a self-contained `file://`-safe document.

Operational limit:

- environments that block localhost port binding cannot start `atlas open`,
  even though the HTML wrapper itself remains valid

### Phase 5 selective info contract

`atlas info` is intentionally selective:

- resolves one table through `get_tables(schema)`
- fetches columns, FKs, and indexes only when requested
- always fetches row estimate and size for the selected table
- never writes files or produces sigilo artifacts

Output formats:

- `text`: human-readable, terminal-oriented summary
- `json`: automation-safe structured output
- `yaml`: automation-safe structured output with an internal fallback serializer

### Phase 6 heuristic analysis contract

Phase 6 adds the `atlas.analysis` package and exports:

- `TableClassifier`
- `TableClassification`
- `TableScorer`
- `TableScore`
- `ScoreBreakdown`
- `AnomalyDetector`
- `StructuralAnomaly`
- `AnomalySeverity`

The analysis stack operates only on `IntrospectionResult` and `TableInfo`
metadata. It does not open database connections by itself.

Classification behavior:

- writes `heuristic_type` and `heuristic_confidence` back into each table
- recognizes staging, config, pivot, log, fact, domain_main, dimension, and
  unknown structures
- uses weighted deterministic rules rather than semantic inference

Scoring behavior:

- writes `relevance_score` back into each table
- ranks tables by structural importance and data quality proxies

### Phase 8 local-AI contract

Phase 8 adds the `atlas.ai` package and exports:

- `AIConfig`
- `ModelInfo`
- `LocalLLMClient`
- `OllamaClient`
- `LlamaCppClient`
- `OpenAICompatibleClient`
- `SamplePreparer`
- `ResponseParser`
- `SemanticEnricher`

Provider behavior:

- Ollama uses `/api/version` and `/api/generate`
- llama.cpp HTTP server uses `/health` and `/completion`
- local OpenAI-compatible servers use `/v1/models` and
  `/v1/chat/completions`
- `build_client()` respects an explicit provider choice
- `auto_detect_client()` probes the canonical provider order only when the
  provider is `auto`

Semantic firewall behavior:

- `SamplePreparer` never opens database connections
- detected email, CPF, and CNPJ values are replaced with `[PATTERN: ...]`
  before prompt generation even in `PrivacyMode.normal`
- `stats_only` and `no_samples` always suppress row-value samples
- prompt context is capped by distinct-value and character budgets

Semantic enrichment behavior:

- `SemanticEnricher.enrich_table()` writes semantic fields back into `TableInfo`
- `SemanticEnricher.enrich_column()` writes semantic fields back into
  `ColumnInfo`
- `ResponseParser` accepts plain JSON, fenced markdown JSON, and JSON wrapped
  by surrounding text
- only `AITimeoutError` is retried, with a fixed backoff of `1s` then `2s`

Operational limits:

- Phase 8 is local-first and does not include remote-provider authentication
- enrichment works on one table or one column at a time
- no cache, batch orchestration, or parallel enrichment exists in this phase

### Phase 9 semantic orchestration contract

Phase 9 extends the semantic surface with:

- `SemanticCache`
- schema-wide `SemanticEnricher.enrich_schema(...)`
- semantic sigilo rebuild through
  `DatamapSigiloBuilder.rebuild_with_semantics(...)`
- the `atlas enrich` CLI

Cache behavior:

- semantic cache is stored as `.semantic_cache.json` under the output
  directory
- reuse is gated by structural signatures over schema, table, and column
  structure
- cache writes are atomic and stale payloads are ignored automatically when the
  structure changes

Schema enrichment behavior:

- enrichment runs with table-level parallelism
- a live connector is used for sampling only when one is available and the
  resolved `PrivacyMode` permits samples
- table-level AI failures degrade into semantic failure text instead of
  aborting the whole schema
- column-level AI failures are skipped without aborting the parent table
- `tables_only=True` enriches tables while leaving column semantic fields empty

Sigilo semantic behavior:

- table wrappers now expose semantic `data-*` attributes
- column wrappers now expose semantic `data-*` attributes
- the instant hover tooltip shows semantic descriptions, role/domain metadata,
  and confidence when present
- Phase 9 preserves the approved sigilo visual formulation and does not change
  node geometry, layout rules, or style presets

`atlas enrich` behavior:

- accepts exactly one of `--sigil`, `--db`, or `--config`
- supports `--schema`, `--table`, `--parallel`, `--force`, `--tables-only`,
  and `--dry-run`
- validates local AI availability before enrichment starts
- writes `<stem>_semantic.sigil`, `<stem>_semantic.svg`, and
  `<stem>_semantic_meta.json`
- keeps `.sigil` as JSON serialization of `IntrospectionResult`
- groups ranked tables by previously assigned heuristic domain

Anomaly behavior:

- reports structural issues with info/warning severities
- includes per-column anomalies when applicable
- excludes view-like objects from PK/index warnings

Operational limits:

- heuristic outputs are only as good as the connector metadata already present
- comment-aware scoring depends on connector support for table comments
- no Phase 6 rule uses live sample rows or workload telemetry

### Phase 10 natural-language QA contract

Phase 10 extends the public package with:

- `AtlasQA`
- `QACandidate`
- `QAResult`
- `EmbeddingGenerator`
- `VectorSearch`
- `VectorIndexEntry`
- `VectorCandidate`

The `atlas ask` command operates over one of three structural inputs:

- `--sigil PATH`
- `--db URL`
- `--config PATH`

Its ranking stack is layered:

1. local LLM interpretation of the question into search hints
2. structural validation through `AtlasSearch`
3. semantic overlap against `semantic_*` fields already written into the
   metadata
4. heuristic contribution from `relevance_score`, `heuristic_type`, and
   `heuristic_confidence`

Vector search is optional:

- when embeddings are supported, Atlas can rank semantic neighbors from a
  `<stem>.embeddings` sidecar
- when embeddings are unavailable, `atlas ask` degrades to hybrid structural +
  semantic + heuristic ranking without aborting

Operational limit:

- Phase 10 answers with ranked metadata targets and optional SQL suggestions,
  but it does not execute SQL

### Phase 11 snapshot and history contract

Phase 11 extends the package with:

- `SnapshotManifest`
- `AtlasSnapshot`
- `SnapshotDiff`
- `SnapshotDiffReport`
- `AtlasHistory`

Snapshot archive rules:

- `.atlas` is a ZIP-backed offline artifact
- required members are:
  - `manifest.json`
  - `schema.json`
  - `sigilo.svg`
  - `sigilo.sigil`
  - `scores.json`
  - `anomalies.json`
- `semantics.json` is optional
- `peek_manifest(...)` reads only the lightweight header

Snapshot diff rules:

- compares snapshots offline only
- detects:
  - added and removed tables
  - added and removed columns
  - physical type changes
  - significant volume drift
  - added and removed foreign-key relations
- reuses stored sigilo SVGs side by side in the HTML diff report

History rules:

- local naming format is `database_YYYYMMDD_HHMMSS.atlas`
- `latest` resolves to the newest valid snapshot
- date-prefix references must be unambiguous
- `atlas history open` reuses the existing local Atlas viewer flow

### Phase 7 search and report contract

Phase 7 adds the `atlas.search` package and exports:

- `EntityType`
- `SearchResult`
- `AtlasSearch`
- `CandidateRef`
- `DiscoveryResult`
- `AtlasDiscovery`

Search behavior:

- `AtlasSearch` works entirely in memory over an existing
  `IntrospectionResult`
- `search_tables()` ranks table hits by lexical match strength
- `search_columns()` ranks column hits by names, type hints, and comments
- `search_schema()` returns a mixed result stream across schemas, tables, and
  columns

Discovery behavior:

- `AtlasDiscovery` turns near-natural-language prompts into ranked candidate
  tables
- stop-word removal is bilingual (PT-BR and English)
- synonym expansion is driven by a fixed heuristic domain map
- FK hub tables receive a topology bonus when they are referenced by other
  current candidates
- discovery produces a reasoning string and a normalized confidence value

Report behavior:

- `atlas report` writes a single offline-openable HTML file
- report generation reuses Phase 6 classification, scoring, and anomaly APIs
- live mode accepts `--db` or `--config`
- snapshot mode accepts `--sigil`
- the embedded sigilo section uses the existing Phase 4 hover contract without
  changing the visual rendering rules
- when the native sigilo binding is unavailable, or `--no-sigilo` is used, the
  report shows a warning block instead of failing

## Connector behavior

`BaseConnector` defines:

- lifecycle: `connect`, `disconnect`, `ping`, `session`
- metadata: schemas, tables, columns, foreign keys, indexes
- statistics: row counts, sizes, NULL counts, distinct estimates
- sampling: privacy-aware row access
- orchestration: `introspect_schema` and `introspect_all`

The base class marks indexed columns and foreign-key source columns during
schema introspection and computes incoming foreign-key degree during full
introspection.

## PostgreSQL support in Phase 1

`PostgreSQLConnector` adds:

- pooled connections through `psycopg2.pool.ThreadedConnectionPool`
- read-only sessions with `statement_timeout` and `lock_timeout`
- schema discovery from `information_schema.schemata`
- table discovery for base tables, views, and materialized views
- column metadata from `information_schema.columns`, `pg_constraint`, and
  `pg_description`
- row-count estimates from `pg_stat_user_tables` with `pg_class.reltuples`
  fallback
- table size discovery via `pg_relation_size`
- declared foreign keys from `pg_constraint`
- inferred foreign keys for same-schema `<name>_id` columns when no declared FK
  exists
- index discovery from `pg_index`, including uniqueness, primary, partial, and
  access-method metadata
- sample rows via `LIMIT` on smaller tables and `TABLESAMPLE SYSTEM` on larger
  tables
- column statistics from `pg_stats`, including NULL-count and distinct-count
  estimates

Operational limits:

- inferred foreign keys only target same-schema tables and default to target
  column `id`
- masking is driven only by column names
- `pg_stats`-derived statistics are only as fresh as the latest `ANALYZE`
- `TABLESAMPLE SYSTEM` is approximate and may return fewer rows than requested
- non-tabular objects such as sequences, triggers, and check constraints are
  not surfaced yet

## MySQL and MariaDB support in Phase 2

`MySQLConnector` adds:

- pooled connections through `mysql.connector.pooling.MySQLConnectionPool`
- server-version detection for both MySQL and MariaDB
- schema and table discovery from `information_schema`
- table, view, column, comment, FK, and index introspection
- estimated row counts and table sizes from `information_schema.tables`
- privacy-aware row sampling with `ORDER BY RAND()` on smaller tables and
  random-offset sampling on larger tables
- canonical type mapping for MySQL and MariaDB native types
- MariaDB JSON-alias recognition when the catalog exposes `JSON` as
  `LONGTEXT CHECK (json_valid(...))`

Operational limits:

- row counts are catalog estimates and may lag behind recent writes
- MariaDB JSON normalization depends on the `json_valid(column)` check-constraint
  pattern exposed by the catalog
- no trigger, routine, or check-constraint metadata is surfaced as first-class
  Atlas objects in this phase

## SQL Server support in Phase 2

`MSSQLConnector` adds:

- queue-backed connection pooling through `pyodbc`
- SQL Server version and edition detection from `@@VERSION`
- schema, table, view, and synonym discovery from system catalogs
- column comments through `MS_Description` extended properties
- FK and index discovery from `sys.foreign_keys`, `sys.indexes`, and related
  catalog views
- row-count and size estimates from DMVs and allocation metadata
- privacy-aware row sampling through `TOP`, `NEWID()`, and `TABLESAMPLE`
- canonical type mapping for SQL Server native types, including the distinction
  between temporal `datetime*` values and binary `timestamp`/`rowversion`

Operational limits:

- the current test and development path uses `pyodbc` with FreeTDS on macOS
- `TABLESAMPLE` is approximate and may return zero rows, so the connector
  falls back to randomized `TOP` sampling
- check constraints, triggers, procedures, and user-defined table types are not
  surfaced yet

## SQLite support in Phase 0

Phase 0 includes a minimal stdlib-backed SQLite connector so the connector
factory can return a working implementation without external drivers. It is
limited to basic metadata and sampling behavior.

## Sigilo support in Phase 3 and Phase 4

The shared library bundles:

- vendored standalone CCT common and sigilo parse/validate sources
- Atlas-native node/edge SVG rendering code

The runtime entry points are:

- `atlas._sigilo.available()`
- `atlas._sigilo.ping()`
- `atlas._sigilo.render_version()`
- `atlas._sigilo.RenderContext`
- `atlas._sigilo.RenderContext.compute_layout_force()`

Operational limits:

- local validation in this repository covers Unix-like native toolchains
- Windows packaging hooks are present but were not exercised in the local
  regression block
- the fallback renderer is intentionally simpler in layout, but preserves SVG
  metadata, hover content, wrapper classes, and style-driven canvas sizing
- hover metadata is implemented with inline vanilla JavaScript and expects the
  SVG to be opened in a browser or viewer that executes embedded scripts
- the native force layout is tuned for discovery maps, not for exact graph
  drawing optimality; extremely dense schemas can still produce crossing edges
- `atlas open` depends on local browser access plus the ability to bind an HTTP
  port on `127.0.0.1`
