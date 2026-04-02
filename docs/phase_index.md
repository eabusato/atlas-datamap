# Phase Index

## Phase 0

- 0A: package layout, build system, root CLI, placeholders, and packaging tests
- 0B: connection configuration, parsing, validation, and config integration tests
- 0C: canonical metadata types, base connector, connector factory, and connector integration tests

## Phase 1

- 1A: PostgreSQL connection lifecycle, server version detection, and schema discovery
- 1B: PostgreSQL tables, views, materialized views, columns, row estimates, and size metadata
- 1C: declared and implicit foreign keys, indexes, and redundancy detection
- 1D: privacy-aware row sampling and `pg_stats`-derived column statistics

## Phase 2

- 2A: MySQL and MariaDB connector lifecycle, schema discovery, metadata, and sampling
- 2B: SQL Server connector lifecycle, schema discovery, metadata, and sampling
- 2C: cross-engine canonical type normalization and connector propagation

## Phase 3

- 3A: vendored standalone CCT sigilo/common sources plus the Atlas native SVG renderer
- 3B: ABI-level `cffi` wrapper, `SigiloBuilder`, and Python fallback renderer
- 3C: integrated native build, packaging hooks, Makefile targets, CI workflow, and build checks

## Phase 4

- 4A: `DatamapSigiloBuilder`, style presets, schema-aware node collection, and enriched SVG metadata
- 4B: embedded vanilla-JS hover tooltip injection for node and edge metadata
- 4C: native force-directed layout, Python/CFFI binding, and schema-aware force layout tests

## Phase 5

- 5A: `atlas scan`, introspection orchestration, and snapshot artifact persistence
- 5B: `atlas open`, in-memory HTML wrapping, and local side-panel viewer
- 5C: `atlas info`, selective table inspection, and text/JSON/YAML formatting

## Phase 6

- 6A: heuristic table classification with confidence and signal tracking
- 6B: weighted relevance scoring, ranking, and domain clustering
- 6C: structural anomaly detection and anomaly summaries

## Phase 7

- 7A: textual metadata search and the `atlas search` CLI
- 7B: heuristic domain discovery over metadata graphs
- 7C: stand-alone HTML health reports and the `atlas report` CLI

## Phase 8

- 8A: local LLM client abstraction, provider detection, and error contracts
- 8B: semantic-firewall sample preparation with PII redaction and token-budget control
- 8C: prompt templates, resilient JSON parsing, timeout retry, and semantic metadata enrichment

## Phase 9

- 9A: persistent semantic cache, schema-wide enrichment, and table-level parallelism
- 9B: semantic `data-*` injection in sigilo wrappers and semantic hover content
- 9C: `atlas enrich` CLI for `.sigil`, live database, and TOML-driven enrichment

## Phase 10

- 10A: hybrid natural-language QA over structural, semantic, and heuristic metadata
- 10B: optional local embeddings, vector search, and `.embeddings` persistence
- 10C: `atlas ask` CLI with `.sigil`, live database, interactive mode, and JSON output

## Phase 11

- 11A: `.atlas` snapshot archives with manifest peek and offline reload
- 11B: structural snapshot diff, relation drift, and offline HTML diff reports
- 11C: local snapshot history, `latest` resolution, and history-driven open/diff flows

## Phase 12

- 12A: standalone offline HTML sigilo export with preserved SVG contract
- 12B: structured JSON, CSV, and Markdown export from `.sigil` and `.atlas`
- 12C: executive HTML reporting through `atlas report --style executive`

## Phase 13

- 13A: hardened SQLite connector with PRAGMA metadata, real row counts, and schema file-size accounting
- 13B: MariaDB differentiation on the MySQL path with routines, sequence fallback, and serializable schema metadata
- 13C: degraded generic SQLAlchemy connector with `generic+<dialect>` URL support

## Phase 14

- 14A: stable public `Atlas` SDK facade and artifact wrapper
- 14B: sigilo regression baselines, public-API regression suite, and CI expansion
- 14C: release-ready packaging, distribution workflows, and publishing documentation

## Documentation map

- Architecture: [architecture.md](architecture.md)
- Getting started: [getting_started.md](getting_started.md)
- Privacy: [privacy.md](privacy.md)
- System manual: [system_manual.md](manuals/system_manual.md)
- Developer manual: [developer_manual.md](manuals/developer_manual.md)
- Native build guide: [building_c_extension.md](building_c_extension.md)
- Publishing guide: [publishing.md](publishing.md)
- Phase 0 spec: [phase_0_contracts.md](specs/phase_0_contracts.md)
- Phase 1 spec: [phase_1_contracts.md](specs/phase_1_contracts.md)
- Phase 2 spec: [phase_2_contracts.md](specs/phase_2_contracts.md)
- Phase 3 spec: [phase_3_contracts.md](specs/phase_3_contracts.md)
- Phase 4 spec: [phase_4_contracts.md](specs/phase_4_contracts.md)
- Phase 5 spec: [phase_5_contracts.md](specs/phase_5_contracts.md)
- Phase 6 spec: [phase_6_contracts.md](specs/phase_6_contracts.md)
- Phase 7 spec: [phase_7_contracts.md](specs/phase_7_contracts.md)
- Phase 8 spec: [phase_8_contracts.md](specs/phase_8_contracts.md)
- Phase 9 spec: [phase_9_contracts.md](specs/phase_9_contracts.md)
- Phase 10 spec: [phase_10_contracts.md](specs/phase_10_contracts.md)
- Phase 11 spec: [phase_11_contracts.md](specs/phase_11_contracts.md)
- Phase 12 spec: [phase_12_contracts.md](specs/phase_12_contracts.md)
- Phase 13 spec: [phase_13_contracts.md](specs/phase_13_contracts.md)
- Phase 14 spec: [phase_14_contracts.md](specs/phase_14_contracts.md)
- Phase 0 release notes: [phase_0.md](release_notes/phase_0.md)
- Phase 1 release notes: [phase_1.md](release_notes/phase_1.md)
- Phase 2 release notes: [phase_2.md](release_notes/phase_2.md)
- Phase 3 release notes: [phase_3.md](release_notes/phase_3.md)
- Phase 4 release notes: [phase_4.md](release_notes/phase_4.md)
- Phase 5 release notes: [phase_5.md](release_notes/phase_5.md)
- Phase 6 release notes: [phase_6.md](release_notes/phase_6.md)
- Phase 7 release notes: [phase_7.md](release_notes/phase_7.md)
- Phase 8 release notes: [phase_8.md](release_notes/phase_8.md)
- Phase 9 release notes: [phase_9.md](release_notes/phase_9.md)
- Phase 10 release notes: [phase_10.md](release_notes/phase_10.md)
- Phase 11 release notes: [phase_11.md](release_notes/phase_11.md)
- Phase 12 release notes: [phase_12.md](release_notes/phase_12.md)
- Phase 13 release notes: [phase_13.md](release_notes/phase_13.md)
- Phase 14 release notes: [phase_14.md](release_notes/phase_14.md)
