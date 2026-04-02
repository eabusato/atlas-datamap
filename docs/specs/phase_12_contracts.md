# Phase 12 Contracts

## Standalone sigilo export contract

Phase 12A adds offline HTML wrapping through:

- `StandaloneHTMLBuilder`
- `atlas export svg`

Supported input sources:

- `.sigil` via `IntrospectionResult.from_json(...)`
- `.atlas` via `AtlasSnapshot.load(...)`

Rules:

- exactly one source is accepted: `--sigil` or `--atlas`
- `.atlas` prefers the stored `sigil_svg` payload
- `.sigil` rebuilds the SVG with the canonical current `SigiloBuilder`
- the exported document is a single offline HTML file with inline CSS, inline
  JavaScript, and embedded inline SVG
- the wrapper never rewrites the SVG node graph or changes the approved visual
  formulation of the sigilo

Operational selectors used by the wrapper:

- `.system-node-wrap[data-table]`
- `.system-schema-wrap[data-schema]`
- `.system-edge-wrap[data-fk-from]`

The wrapper preserves existing `data-*` attributes, including semantic payloads
added in Phase 9.

## Structured export contract

Phase 12B adds `StructuredExporter` and the CLI family:

- `atlas export json`
- `atlas export csv`
- `atlas export markdown`

`StructuredExporter(result, semantics=None)` rules:

- `result` is always `IntrospectionResult`
- optional `semantics` follows the real snapshot shape:
  - `semantics["tables"]["schema.table"]`
  - `semantics["columns"]["schema.table.column"]`
- when no sidecar semantics are provided, the exporter may fall back to the
  `semantic_*` fields already present in `TableInfo` and `ColumnInfo`

Output contracts:

- JSON:
  - preserves `IntrospectionResult.to_dict()` structure
  - injects `semantic_data` in table and column payloads when available
  - uses UTF-8 and `indent=2`
- CSV tables:
  - one row per table
  - stable English headers
- CSV columns:
  - one row per column
  - stable English headers
  - includes flags and metric fields such as `Distinct Count` and `Null Rate`
- Markdown:
  - groups by schema
  - emits one section per table
  - includes a column dictionary table
  - does not depend on embedded HTML

## Executive report contract

Phase 12C extends `atlas report` instead of creating a parallel reporting CLI.

New option:

```bash
atlas report --style executive ...
```

Allowed styles:

- `health` (existing Phase 7 contract)
- `executive` (new Phase 12 contract)

Input rules:

- exactly one source is accepted:
  - `--db` / `--config`
  - `--sigil`
  - `--atlas`

`ExecutiveReportGenerator` rules:

- HTML is fully offline and self-contained
- no external CSS, JS, fonts, or CDN assets are allowed
- the executive report does not embed heavy interactive sigilo content
- when `.atlas` is used:
  - `snapshot.result` is the structural source
  - `snapshot.scores` is reused when present
  - `snapshot.anomalies` is reused when present
  - `snapshot.semantics` drives semantic coverage and inventory sections
- when `.sigil` or live introspection is used:
  - scores fall back to `TableScorer(result).score_all()`
  - anomalies fall back to `AnomalyDetector().detect(result)`
  - semantic sections degrade gracefully when semantic metadata is absent

Required executive sections:

- Overview
- Schemas
- Top Tables
- Anomalies
- Recommendations
- Optional Semantic Coverage

## Compatibility rules

- Phase 12 does not modify the sigilo renderer formula, layout grammar, or
  hover model
- Phase 12 keeps `atlas report` backward-compatible in `--style health`
- Phase 12 does not change the `.sigil` or `.atlas` persistence formats
