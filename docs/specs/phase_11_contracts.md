# Phase 11 Contracts

## Snapshot archive contract

Phase 11 adds offline persistence through the ZIP-backed `.atlas` format.

Public exports now include:

- `SnapshotManifest`
- `AtlasSnapshot`
- `ColumnTypeChange`
- `VolumeChange`
- `SchemaDiff`
- `SnapshotDiff`
- `SnapshotDiffReport`
- `AtlasHistory`

`AtlasSnapshot.from_result(...)` contract:

- accepts an in-memory `IntrospectionResult`
- accepts rendered `sigil_svg` and textual `sigil_payload`
- computes `scores` through `TableScorer` when omitted
- computes `anomalies` through `AnomalyDetector` when omitted
- derives `semantics` from `semantic_*` metadata when omitted

Archive structure:

- required:
  - `manifest.json`
  - `schema.json`
  - `sigilo.svg`
  - `sigilo.sigil`
  - `scores.json`
  - `anomalies.json`
- optional:
  - `semantics.json`

Serialization rules:

- `schema.json` stores `IntrospectionResult.to_json(indent=2)`
- `sigilo.sigil` preserves the current Atlas `.sigil` payload as text
- `sigilo.svg` is stored as UTF-8 text
- `scores.json` stores the JSON form of `TableScore.to_dict()`
- `anomalies.json` stores the JSON form of `StructuralAnomaly.to_dict()`

Manifest rules:

- `created_at` is UTC ISO-8601
- `contents` enumerates the files present in the archive
- `peek_manifest(...)` reads only `manifest.json`

Operational limits:

- `.atlas` is not incremental
- encryption is not part of Phase 11
- snapshot loading is offline-only and never reconnects to the database

## Snapshot diff contract

`SnapshotDiff.compare(before, after)` operates only on two loaded
`AtlasSnapshot` instances.

It produces:

- `added_tables`
- `removed_tables`
- `added_columns`
- `removed_columns`
- `type_changes`
- `volume_changes`
- `new_relations`
- `removed_relations`

Diff rules:

- table identity is `schema.table`
- type mutations compare `ColumnInfo.native_type`, not `canonical_type`
- volume drift is emitted only when:
  - baseline rows are greater than `1000`
  - absolute delta is at least `20%`
- relations are compared through a deterministic signature over source/target
  tables and columns

`SnapshotDiffReport` contract:

- renders a stand-alone HTML report
- includes summary counts plus sections for tables, columns, types, volume, and
  relations
- reuses stored sigilo SVGs side by side
- does not alter the approved sigilo visual formulation

## CLI contracts

### `atlas diff`

Supported form:

```bash
atlas diff before.atlas after.atlas --output diff.html
```

Rules:

- both inputs are loaded as `.atlas`
- output is required
- HTML is generated offline
- `stdout` may include a short summary
- failures return a non-zero exit code

### `atlas history`

Phase 11 adds a grouped CLI for local snapshot timelines.

Supported forms:

```bash
atlas history list --dir ./snapshots
atlas history diff --dir ./snapshots --from 20260320 --to latest --output diff.html
atlas history open --dir ./snapshots --date latest
```

`AtlasHistory(directory)` rules:

- `build_snapshot_name(database, created_at)` yields
  `database_YYYYMMDD_HHMMSS.atlas`
- `list_snapshots()` returns valid snapshots sorted newest first
- `latest()` returns the newest snapshot path or `None`
- `resolve_snapshot(reference)` supports:
  - `latest`
  - exact file names
  - unambiguous `YYYYMMDD` date references

History rules:

- `list` uses `peek_manifest()` only
- corrupted archives are ignored during listing
- ambiguous date references fail clearly
- `open` reuses the current local Atlas viewer flow rather than creating a new
  viewer implementation

## Compatibility rules

- Phase 11 does not change the `.sigil` payload contract
- Phase 11 does not change sigilo layout, style, or hover behavior
- Phase 11 remains compatible with the current `.sigil`-based `open`, `ask`,
  `report`, and `enrich` workflows
