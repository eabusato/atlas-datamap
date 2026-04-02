# Phase 5 Contracts

## CLI generation contract

Phase 5 turns the previously registered CLI placeholders into working Atlas
commands:

- [`atlas/cli/scan.py`](../../atlas/cli/scan.py)
- [`atlas/cli/open.py`](../../atlas/cli/open.py)
- [`atlas/cli/info.py`](../../atlas/cli/info.py)
- [`atlas/cli/_common.py`](../../atlas/cli/_common.py)
- [`atlas/cli/_info_format.py`](../../atlas/cli/_info_format.py)

The command surface now includes:

- `atlas scan`
- `atlas open`
- `atlas info`

The commands share configuration resolution through:

- `--db <connection-url>`
- `--config <atlas.toml>`
- `ATLAS_*` environment variables as a fallback when neither CLI source is
  provided

## Introspection orchestration contract

Phase 5A adds:

- [`atlas/introspection/runner.py`](../../atlas/introspection/runner.py)
- [`atlas/export/snapshot.py`](../../atlas/export/snapshot.py)

`IntrospectionRunner` owns connector lifecycle for full-database scans and
emits progress through:

- `_ProgressEvent(stage, message, current=0, total=0, elapsed_ms=0)`
- `ProgressCallback`
- `IntrospectionError`

Operational behavior:

- connect/disconnect always happen inside `IntrospectionRunner.run()`
- the runner emits ordered stage updates for `connect`, `schemas`, `tables`,
  `columns`, and `relations`
- schema filtering is applied from `AtlasConnectionConfig.schema_filter` and
  `schema_exclude`
- per-table failures are re-raised as `IntrospectionError` including the
  failing `schema.table`
- the final `IntrospectionResult` always includes
  `fk_in_degree_map` and `introspected_at`

## Artifact persistence contract

`save_artifacts(result, svg_bytes, output_dir, stem=None)` persists exactly
three files:

- `{stem}.svg`
- `{stem}.sigil`
- `{stem}_meta.json`

Rules:

- output directories are created automatically
- the default stem is derived from `result.database`
- the stem is normalized to ASCII, replaces `/`, `\`, `:`, and spaces with
  `_`, removes unsupported characters, and truncates to 64 characters
- `.sigil` stores compact JSON equivalent to `result.to_dict()`
- `_meta.json` stores pretty JSON equivalent to `result.to_json(indent=2)`

`atlas scan` uses the basename of path-like database names when building the
artifact stem, so SQLite database paths produce filenames such as
`scan.db.svg` instead of path-derived absolute names.

## `atlas scan` contract

`atlas scan` now supports:

- `--db`
- `--config`
- `--schema`
- `--output`
- `--style`
- `--layout`
- `--privacy`
- `--dry-run`
- `--force`
- `--quiet`

Behavior:

- uses `IntrospectionRunner` plus `DatamapSigiloBuilder`
- `--schema` is parsed as a comma-separated schema filter and overrides the
  runtime config filter
- `--privacy` overrides the runtime privacy mode
- `--dry-run` introspects and renders but does not write files
- `--force` is required when any target artifact already exists
- `--quiet` suppresses progress events but keeps the final summary

Operational limits:

- rendering still depends on the current Sigilo backends and their existing
  Phase 3/4 limits
- schema filtering can legitimately remove every renderable table, in which
  case the command fails instead of writing empty artifacts

## `atlas open` contract

Phase 5B adds:

- [`atlas/sigilo/panel.py`](../../atlas/sigilo/panel.py)

`PanelBuilder(svg_bytes, db_name="")` returns a standalone HTML document that:

- embeds the input SVG inline in `<main id="atlas-canvas">`
- renders a side panel in `<aside id="atlas-panel">`
- builds a schema → table tree from `g.system-node-wrap[data-schema][data-table]`
- exposes live text filtering through `#atlas-search`
- highlights and scrolls to the clicked SVG node by toggling `atlas-selected`

`AtlasLocalServer` serves that HTML through stdlib `http.server`:

- `/` returns `200 text/html`
- `/favicon.ico` returns `204`
- the server opens the browser through `webbrowser.open()`
- the foreground command stops on `Ctrl+C`

Operational limits:

- the side panel depends on the Phase 4 SVG `data-*` attributes being present
- local port binding can be blocked by sandboxed or locked-down environments;
  in those cases the generated HTML is still valid, but the foreground server
  cannot start

## `atlas info` contract

Phase 5C adds:

- selective metadata loading through `_fetch_table_info()`
- `TableNotFoundError`
- text, JSON, and YAML renderers in
  [`atlas/cli/_info_format.py`](../../atlas/cli/_info_format.py)

`atlas info` supports:

- `--db`
- `--config`
- `--table`
- `--format text|json|yaml`
- `--columns / --no-columns`
- `--indexes / --no-indexes`
- `--fks / --no-fks`

Behavior:

- never runs full-database `introspect_all()`
- resolves the target table through `get_tables(schema)`
- fetches columns, foreign keys, and indexes only when their flags are enabled
- always fetches row estimates and table size for the selected table
- defaults bare `--table orders` references to schema `public`

YAML behavior:

- uses PyYAML when installed
- falls back to an internal serializer for `dict`, `list`, scalar, and `null`
  values produced by `TableInfo.to_dict()`

Operational limits:

- defaulting to schema `public` is convenient for PostgreSQL-style usage but
  may not match the real default schema of every engine
- the text renderer is optimized for terminal readability and not for stable
  machine parsing; JSON and YAML are the automation-safe formats
