# Phase 9 Release Notes

## Scope

Phase 9 turns semantic enrichment into an operator-facing workflow. The Atlas
package now supports schema-wide enrichment with persistent caching, semantic
hover payloads in the sigilo, and a working `atlas enrich` CLI.

## Delivered

- Added `SemanticCache` as a persistent JSON cache stored under the output
  directory.
- Extended `SemanticEnricher` with:
  - cache reuse
  - structural invalidation by table and column signatures
  - schema-wide enrichment
  - table-level parallelism through `ThreadPoolExecutor`
  - graceful degradation when sampling or AI calls fail
- Added semantic `data-*` injection for tables and columns in the canonical
  sigilo builder path, preserving parity between the native renderer and the
  Python fallback.
- Extended the instant hover tooltip to display semantic table and column
  content when present.
- Added `DatamapSigiloBuilder.rebuild_with_semantics(...)`.
- Replaced the `atlas enrich` placeholder with a functional CLI command that
  supports:
  - `--sigil`
  - `--db`
  - `--config`
  - `--schema`
  - `--table`
  - `--parallel`
  - `--force`
  - `--tables-only`
  - `--dry-run`
- Added Phase 9 integration coverage and wired Phase 9 into
  `tests/run_tests.sh`.
- Updated historical CLI packaging coverage so `atlas enrich --help` is treated
  as a real command surface rather than a placeholder.

## Notes

- Phase 9 preserves the current sigilo visual formulation. It only enriches
  metadata, hover content, and exported artifacts.
- The semantic cache is local and file-based. There is no remote cache backend
  in this phase.
- `atlas ask` remains a later-phase placeholder.
