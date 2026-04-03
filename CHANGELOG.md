# Changelog

## 1.0.1

- Fixed editable-install and build-backend bootstrap issues across Linux, macOS, and Windows CI.
- Fixed Windows SQLite URL normalization for drive-letter paths in the public SDK and connector flow.
- Fixed Windows-native sigilo memory release by exporting and using the library-owned free-buffer API.
- Reduced the default Linux CI regression flow to run the full suite once instead of replaying the same coverage repeatedly.
- Fixed release packaging so native wheel builds no longer leak `atlas/_c/build` into distributions and macOS wheel builds honor the target architecture.
- Made GitHub Pages deployment non-blocking for package publication workflows when Pages is not enabled.

## 1.0.0

- Phase 0: established package layout, CLI bootstrap, config parsing, metadata types, and connector abstractions.
- Phase 1: delivered the PostgreSQL connector with catalog introspection, relationships, indexes, stats, and privacy-aware sampling.
- Phase 2: added MySQL, MariaDB, and SQL Server connectors plus cross-engine canonical type mapping.
- Phase 3: vendored the native Sigilo renderer, exposed the CFFI bridge, and integrated native build hooks.
- Phase 4: turned sigilo into a schema-aware database datamap with rich hover metadata and stable visual layouts.
- Phase 5: delivered the operational CLI surface for scan, open, and targeted info queries.
- Phase 6: added deterministic classification, scoring, and anomaly detection over introspected metadata.
- Phase 7: added textual search, discovery helpers, and stand-alone HTML reporting.
- Phase 8: integrated local LLM clients, privacy-preserving prompt contexts, and semantic enrichment of tables and columns.
- Phase 9: added persistent semantic caching, semantic sigilo attributes, and the `atlas enrich` workflow.
- Phase 10: added hybrid natural-language QA, optional embeddings, vector search sidecars, and the `atlas ask` workflow.
- Phase 11: added portable `.atlas` snapshots, structural diffs, and local snapshot history commands.
- Phase 12: added standalone sigilo export, structured JSON/CSV/Markdown export, and executive reporting.
- Phase 13: hardened SQLite support, differentiated MariaDB on the MySQL path, and added the degraded generic SQLAlchemy connector.
- Phase 14: delivered the public Python SDK facade, sigilo regression baselines, integrated CI expansion, and release-ready packaging artifacts.
