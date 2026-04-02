# Phase 9 Contracts

## Semantic orchestration package contract

Phase 9 turns the Phase 8 local-AI primitives into a schema-wide enrichment
pipeline and exposes the result through the sigilo and CLI surfaces.

Files added or materially extended in this phase:

- [`atlas/ai/cache.py`](../../atlas/ai/cache.py)
- [`atlas/ai/enricher.py`](../../atlas/ai/enricher.py)
- [`atlas/sigilo/builder.py`](../../atlas/sigilo/builder.py)
- [`atlas/sigilo/datamap.py`](../../atlas/sigilo/datamap.py)
- [`atlas/sigilo/hover.py`](../../atlas/sigilo/hover.py)
- [`atlas/cli/enrich.py`](../../atlas/cli/enrich.py)

Public exports now include:

- `SemanticCache`
- `SemanticEnricher`

The phase preserves the approved sigilo visual formulation. Phase 9 enriches
metadata and hover content without changing layout geometry, node classes, or
the established database-sigilo visual language.

## Phase 9A cache and schema-pipeline contract

`SemanticCache(cache_dir)` is a JSON-backed cache stored at:

- `<cache_dir>/.semantic_cache.json`

Persistence behavior:

- the file is loaded automatically during construction
- missing, malformed, or non-dict payloads reset the cache to an empty store
- writes are atomic through a sibling `*.tmp` file followed by replace

Structural signature behavior:

- `build_table_signature(table)` hashes:
  - `table.schema`
  - `table.name`
  - each column name
  - each column native type
  - `is_nullable`
  - `is_primary_key`
  - `is_foreign_key`
- `build_column_signature(table, column)` hashes the same structural subset for
  the single column
- semantic fields, comments, scores, and sample values are not part of the
  signature

Cache storage rules:

- table payloads are keyed by `table.qualified_name`
- column payloads are nested under the owning table record
- changing the structural signature invalidates old payload reuse
- `invalidate_table(table)` removes both table and column semantic payloads for
  that table

`SemanticEnricher` now supports:

- `enrich_table(table, sample_rows_or_connector, privacy_mode, force_recompute=False)`
- `enrich_column(table, column, sample_rows_or_connector, privacy_mode, force_recompute=False)`
- `enrich_schema(schema, connector, privacy_mode, parallel_workers=4, force_recompute=False, tables_only=False, on_table_complete=None)`

Input resolution rules:

- `sample_rows_or_connector` may be:
  - a concrete `Sequence[dict[str, Any]]`
  - a live `BaseConnector`
  - `None`
- when a connector is supplied and `privacy_mode.allows_samples` is false, the
  enricher uses empty samples instead of querying rows
- connector-driven row sampling is capped to the smaller of connector
  `sample_limit` and `20`

Schema orchestration rules:

- table work runs in a `ThreadPoolExecutor`
- concurrency is table-scoped, not column-scoped
- `parallel_workers < 1` raises `ValueError`
- empty schemas return immediately and still save cache state when a cache is
  configured

Failure behavior:

- sampling failures degrade to `[]` and do not abort enrichment
- table-level `AIConnectionError`, `AITimeoutError`, and `AIGenerationError`
  set:
  - `table.semantic_short = "Semantic analysis failed"`
  - `table.semantic_detailed = <error text or fallback>`
  - `table.semantic_confidence = 0.0`
- column-level AI failures are skipped and do not abort the table or schema
- successful schema runs persist cache at the end of `enrich_schema()`

`tables_only=True` contract:

- table semantics are populated normally
- column semantics are not requested

## Phase 9B sigilo semantic-surface contract

Phase 9B extends the existing sigilo metadata surface without changing the
approved visual structure.

The semantic injection point is post-render processing inside
[`atlas/sigilo/builder.py`](../../atlas/sigilo/builder.py).
This means both renderer paths receive the same semantic payload surface:

- native C renderer
- Python fallback renderer

Injected table wrapper attributes:

- `data-semantic-short`
- `data-semantic-detailed`
- `data-semantic-role`
- `data-semantic-domain`
- `data-semantic-confidence`

Injected column wrapper attributes:

- `data-semantic-short`
- `data-semantic-detailed`
- `data-semantic-role`
- `data-semantic-confidence`

Operational rules:

- semantic attributes are added only when semantic values are present
- injected values are HTML-escaped before insertion
- existing structural `data-*` attributes remain unchanged
- wrapper classes such as `system-node-wrap`, `system-edge-wrap`, and
  `system-column-wrap` remain stable

`DatamapSigiloBuilder.rebuild_with_semantics(result=None)` contract:

- reuses the canonical builder path already used by `build()`
- optionally swaps the in-memory `IntrospectionResult`
- preserves layout, style, and sizing behavior
- emits SVG with semantic `data-*` attributes and the existing instant-hover
  script

Hover contract additions:

- node hover now shows semantic short description, detailed description, domain,
  role, and confidence when present
- column hover now shows semantic short description, detailed description, role,
  and confidence when present
- edge, schema, and structural hover content remain intact
- Phase 9 does not reintroduce delayed browser-native `<title>` tooltips

## Phase 9C `atlas enrich` CLI contract

`atlas enrich` is now a real command.

Supported input modes:

- `--sigil PATH`
- `--db URL`
- `--config PATH`

Validation rules:

- exactly one of `--sigil`, `--db`, or `--config` is required
- `--table` requires `--schema`
- `--parallel` must be `>= 1`

Offline `--sigil` behavior:

- loads `IntrospectionResult` from JSON through `IntrospectionResult.from_json`
- applies `--schema` / `--table` filters in memory
- enriches with `connector=None`
- uses `PrivacyMode.no_samples`

Live `--db` / `--config` behavior:

- resolves connection config through the existing CLI config helpers
- builds the connector through `get_connector(...)`
- opens `connector.session()`
- introspects through `connector.introspect_all()`
- uses the resolved connection `privacy_mode` during sampling

AI initialization behavior:

- `AIConfig.from_file(...)` is used when `--config` is present
- otherwise `AIConfig()` defaults are used
- `build_client(ai_config)` is the only supported client factory path
- the command aborts before enrichment when `client.is_available()` is false
- model/provider info is echoed to the operator on successful startup

Artifact behavior:

- output directory is created on non-dry runs
- cache lives in the output directory as `.semantic_cache.json`
- persisted files are:
  - `<stem>_semantic.sigil`
  - `<stem>_semantic.svg`
  - `<stem>_semantic_meta.json`
- for `.sigil` input, `<stem>` comes from the input file stem
- otherwise `<stem>` comes from `IntrospectionResult.database`, falling back to
  `atlas`

Operator-progress behavior:

- one progress bar is shown per schema
- per-table completion lines are emitted during schema processing

`--dry-run` behavior:

- does not initialize the AI client
- does not write cache or artifacts
- shows the number of schemas, tables, and columns that would be processed

Operational limits:

- Phase 9 does not implement embeddings, vector search, or natural-language QA
- semantic cache is local JSON only; there is no external cache backend
- schema enrichment is bounded to one process and thread-level parallelism
