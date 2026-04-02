# Phase 10 Release Notes

## Scope

Phase 10 turns Atlas into a natural-language database discovery tool. It adds
hybrid QA ranking, optional local embeddings, and the real `atlas ask` CLI.

## Delivered

- Added `AtlasQA`, `QACandidate`, and `QAResult` for hybrid ranking over:
  - structural search
  - semantic enrichment
  - heuristic relevance
- Added `EmbeddingGenerator` for local embedding backends, with Phase 10
  support for:
  - Ollama `/api/embeddings`
  - optional OpenAI-compatible `/v1/embeddings`
- Added `VectorSearch`, `VectorIndexEntry`, and `VectorCandidate` with
  pure-Python cosine similarity and JSON sidecar persistence in
  `<stem>.embeddings`.
- Replaced the `atlas ask` placeholder with a functional CLI that supports:
  - `--sigil`
  - `--db`
  - `--config`
  - `--interactive`
  - `--format text|json`
  - `--no-embeddings`
- Added lightweight interactive context retention for the last 3 turns in
  `atlas ask`.
- Updated Phase 0A packaging coverage so `atlas ask --help` is treated as a
  real command surface.
- Wired Phase 10A, 10B, and 10C into `tests/run_tests.sh` and pytest markers.

## Notes

- Phase 10 does not modify the approved sigilo visual formulation.
- Natural-language ranking remains grounded in `IntrospectionResult`,
  `semantic_*`, and heuristic metadata rather than free-form LLM inference.
- Embeddings are optional at runtime. `atlas ask` continues to function with
  10A-only ranking when vector search is unavailable.
