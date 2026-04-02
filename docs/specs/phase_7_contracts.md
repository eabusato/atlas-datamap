# Phase 7 Contracts

## Search package contract

Phase 7 adds the search and reporting surface:

- [`atlas/search/types.py`](../../atlas/search/types.py)
- [`atlas/search/textual.py`](../../atlas/search/textual.py)
- [`atlas/search/discovery.py`](../../atlas/search/discovery.py)
- [`atlas/export/report.py`](../../atlas/export/report.py)
- [`atlas/cli/search.py`](../../atlas/cli/search.py)
- [`atlas/cli/report.py`](../../atlas/cli/report.py)

Public exports now include:

- `EntityType`
- `SearchResult`
- `AtlasSearch`
- `CandidateRef`
- `DiscoveryResult`
- `AtlasDiscovery`
- `HTMLReportGenerator`

The whole phase remains deterministic and offline. No network calls, LLMs, or
external services are used.

## Phase 7A textual search contract

`AtlasSearch(result)` is an in-memory search engine over `IntrospectionResult`.

Phase 7A exposes:

- `search_tables(query, schema_filter=None, type_filter=None)`
- `search_columns(query, schema_filter=None)`
- `search_schema(query)`

`SearchResult` contains:

- `entity_type`
- `schema`
- `table`
- `column`
- `score`
- `reason`
- `qualified_name`

Implemented ranking signals:

- exact table-name token set match
- exact table-name token match
- substring match in names
- native/canonical type hints for columns
- comment match

Filtering rules:

- `schema_filter` restricts table or column search to one schema
- `type_filter` restricts table search to one `heuristic_type`
- `--columns` mode on the CLI is exclusive with `--type`

CLI behavior:

- `atlas search --db ... QUERY` performs mixed schema/table/column discovery
- `atlas search --columns ... QUERY` returns only column hits
- `atlas search --schema SCHEMA ...` restricts results to the given schema
- no-match cases return a clear human-readable message instead of failing

Operational limits:

- search works on names and comments only; it does not inspect row values
- search quality depends on prior classification when `type_filter` is used

## Phase 7B heuristic discovery contract

`AtlasDiscovery(result)` translates near-natural-language questions into ranked
table candidates through four deterministic stages:

1. token extraction with bilingual stop-word removal
2. heuristic concept expansion through `HEURISTIC_MAP`
3. candidate accumulation through `AtlasSearch.search_tables()`
4. FK topology bonus plus ranking synthesis

Phase 7B ships:

- `STOP_WORDS`: bilingual PT-BR/EN stop-word set
- `HEURISTIC_MAP`: business-domain synonym map with at least 20 domains
- `_CONFIDENCE_SATURATOR = 40.0`
- `_MAX_CANDIDATES = 5`
- `_TOPOLOGY_BONUS = 0.5`

`CandidateRef` contains:

- `schema`
- `table`
- `score`
- `justification`
- `qualified_name`

`DiscoveryResult` contains:

- `question`
- `candidates`
- `reasoning`
- `confidence`

Discovery rules:

- user tokens are lower-cased, deduplicated, and stripped of stop words
- known synonyms map to a canonical concept key
- unknown tokens fall back to direct search terms
- tables matching multiple concepts accumulate score
- tables referenced by other current candidates receive a single +50% FK hub
  bonus
- the final result is truncated to the top 5 candidates
- `confidence = min(1.0, top_score / 40.0)`

Operational limits:

- discovery is heuristic and metadata-driven, not semantic AI
- topology bonus depends on a populated `fk_in_degree_map`
- ambiguous domain terms can still rank more than one plausible table

## Phase 7C HTML report contract

`HTMLReportGenerator(result)` generates a stand-alone HTML document containing:

1. structural summary
2. top 10 tables by volume
3. top 10 tables by connectivity
4. heuristic type distribution
5. structural anomalies
6. embedded sigilo section

Implemented analysis dependencies:

- `TableClassifier().classify_all(result)`
- `TableScorer(result).score_all()`
- `AnomalyDetector().detect(result)`

The HTML report is:

- single-file
- offline-openable in a browser
- styled with inline CSS only
- scripted with inline JS only
- free of CDN or third-party runtime dependencies

Sigilo embedding rules:

- when the native binding is available, the report embeds the rendered SVG
- when the native binding is unavailable, or when `--no-sigilo` is passed, the
  report shows a warning block instead of failing
- the report reuses the hover behavior contract from Phase 4 without changing
  the sigilo drawing model

CLI behavior:

- `atlas report --db ... --output report.html` introspects live metadata and
  writes the HTML artifact
- `atlas report --sigil PATH --output report.html` reconstructs the report from
  a saved `.sigil` snapshot
- exactly one input source is required: `--db/--config` or `--sigil`

Operational limits:

- report sigilo embedding intentionally depends on the native binding for this
  phase
- the report is read-only; it does not emit SQL fixes or mutation plans
