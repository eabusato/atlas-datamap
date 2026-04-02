# Phase 10 Contracts

## Natural-language discovery package contract

Phase 10 turns the search, semantic enrichment, and CLI layers into an
operator-facing natural-language discovery workflow. The phase adds:

- hybrid QA ranking in [`atlas/search/qa.py`](../../atlas/search/qa.py)
- optional local embeddings in [`atlas/ai/embeddings.py`](../../atlas/ai/embeddings.py)
- pure-Python vector search in [`atlas/search/vector.py`](../../atlas/search/vector.py)
- the real `atlas ask` command in [`atlas/cli/ask.py`](../../atlas/cli/ask.py)

Phase 10 does not change the approved sigilo visual formulation. It consumes
the existing `.sigil` JSON contract and the semantic `semantic_*` metadata
already stored on `TableInfo` and `ColumnInfo`.

## Phase 10A hybrid QA contract

Public search exports now include:

- `AtlasQA`
- `QACandidate`
- `QAResult`

`AtlasQA(result, client, search=None)` rules:

- operates only on an in-memory `IntrospectionResult`
- never opens database connections
- never reads `.sigil` files directly
- uses `LocalLLMClient` only to translate the user question into search hints

LLM interpretation contract:

- prompt is English-only and JSON-first
- accepted keys are:
  - `search_terms: list[str]`
  - `semantic_terms: list[str]`
  - `reasoning: str`
  - `suggested_query: str | null`
- parsing uses `ResponseParser.extract_json(...)`
- if interpretation fails, Atlas falls back to structural tokens extracted from
  the question itself

Ranking contract:

- structural score comes from `AtlasSearch.search_tables(...)`
- structural score is normalized to `0.0..1.0`
- semantic score is computed from overlap against:
  - `table.semantic_short`
  - `table.semantic_detailed`
  - `table.semantic_domain`
  - `table.semantic_role`
  - `column.semantic_short`
  - `column.semantic_detailed`
  - `column.semantic_role`
- semantic score is multiplied by semantic confidence and clamped to `0.0..1.0`
- heuristic score uses:
  - `table.relevance_score`
  - `table.heuristic_type`
  - `table.heuristic_confidence`

Final composition:

```text
final_score =
  structural_score * 0.40 +
  semantic_score   * 0.40 +
  heuristic_score  * 0.20
```

Output rules:

- candidates are sorted by:
  1. `final_score` descending
  2. `semantic_score` descending
  3. `structural_score` descending
  4. `schema.table` ascending
- result set is limited to the top 5 tables
- `QAResult.confidence` is derived from the best final score and reduced when:
  - LLM interpretation failed
  - semantic terms are absent

## Phase 10B embeddings and vector-search contract

Public exports now include:

- `EmbeddingGenerator`
- `VectorIndexEntry`
- `VectorCandidate`
- `VectorSearch`

`EmbeddingGenerator(client)` contract:

- supports local HTTP providers only
- mandatory provider support in this phase:
  - `ollama` via `/api/embeddings`
- optional provider support in this phase:
  - `openai_compatible` via `/v1/embeddings`
- unsupported providers raise `AIGenerationError`
- input text is whitespace-normalized before generation
- output vectors are validated as numeric `list[float]`

`VectorSearch(generator)` contract:

- builds table-level semantic documents from:
  - qualified table name
  - table semantic descriptions
  - semantic domain
  - semantic role
  - heuristic type
  - top column names
- skips tables that do not have semantic or structural material for a useful
  document
- computes cosine similarity in Python with no `numpy`, `faiss`, or external
  vector database

Cosine similarity rules:

- vectors of different sizes return `0.0`
- zero-magnitude vectors return `0.0`
- similarity is clamped to `-1.0..1.0`

Persistence contract:

- embeddings live in a sidecar `<stem>.embeddings`
- format is JSON with:
  - `version`
  - `provider`
  - `model`
  - `entries`
- `VectorSearch.load(...)` rehydrates stored vectors without recomputation

## Phase 10C `atlas ask` CLI contract

`atlas ask` is now a real CLI surface.

Supported structural inputs:

- `--sigil PATH`
- `--db URL`
- `--config PATH`

Validation rules:

- exactly one structural source is required
- in single-shot mode, a question argument is required
- in interactive mode, the initial question is optional

Resolution rules:

- `--sigil` loads `IntrospectionResult` via `IntrospectionResult.from_json(...)`
- `--db` and `--config` introspect live metadata through the connector factory
- loaded results are normalized through:
  - `TableClassifier().classify_all(...)`
  - `TableScorer(...).score_all()`

AI initialization rules:

- `--config` loads `[ai]` through `AIConfig.from_file(...)`
- otherwise `AIConfig()` defaults are used
- `build_client(...)` is the only supported client factory
- the command aborts when `client.is_available()` is false

Vector-search behavior:

- `--no-embeddings` disables the vector layer explicitly
- when embeddings are supported:
  - `atlas ask` loads `<stem>.embeddings` for `.sigil` inputs when present
  - otherwise it builds a vector index in memory
  - for `.sigil` inputs it persists the rebuilt sidecar
- vector failures do not abort the command; they degrade to 10A ranking only

Interactive behavior:

- accepts `quit` and `exit`
- handles `EOFError` and `KeyboardInterrupt` with clean termination
- keeps a lightweight in-memory history of the last 3 turns
- prefixes recent Q/A summaries into the next question context
- does not persist conversation history to disk

Output contract:

- `text` prints:
  - question
  - reasoning
  - confidence
  - ranked candidates
  - suggested SQL when available
  - vector candidates when available
- `json` prints parseable JSON only on `stdout`
- warnings about incompatible or unavailable embeddings go to `stderr`

Operational limits:

- `atlas ask` does not execute the suggested SQL
- Phase 10 keeps vector search table-scoped, not column-scoped
- Phase 10 does not change sigilo layout, styling, or rendering rules
