# Phase 8 Release Notes

## Scope

Phase 8 adds the local AI integration layer used for semantic enrichment of
database metadata, while preserving the existing sigilo rendering contract.

## Delivered

- Added `atlas.ai.types` with `AIConfig`, `ModelInfo`, and the AI-specific
  exception hierarchy.
- Implemented `LocalLLMClient` plus `OllamaClient`, `LlamaCppClient`, and
  `OpenAICompatibleClient` using `urllib.request` only.
- Implemented provider construction through `build_client()` and provider
  probing through `auto_detect_client()`.
- Added `SamplePreparer` as the semantic firewall for prompt context
  preparation, including PII replacement, pattern detection, deduplication, and
  sample-budget enforcement.
- Added strict JSON-oriented prompt templates for tables and columns.
- Implemented `ResponseParser` for plain JSON, markdown-fenced JSON, and
  balanced-brace extraction from mixed text.
- Implemented `SemanticEnricher` with direct mutation of `semantic_*` fields on
  `TableInfo` and `ColumnInfo`.
- Added timeout-only retry with bounded exponential backoff inside
  `SemanticEnricher`.
- Added Phase 8 integration coverage and wired Phase 8 into
  `tests/run_tests.sh`, the phase index, and the manuals.

## Notes

- Phase 8 does not change the SVG or sigilo rendering path.
- The AI surface is local-first and intentionally excludes remote-provider
  authentication and caching.
- CLI enrichment remains out of scope for this phase; the `enrich` command is
  still a later-phase placeholder.
