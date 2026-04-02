# Phase 5 Release Notes

## Scope

Phase 5 delivers the first end-user Atlas CLI workflow on top of the Phase 4
rendering stack:

- database scanning and artifact generation
- local browser viewing with a side panel
- selective table inspection in text, JSON, and YAML

## Delivered

- Added `IntrospectionRunner`, progress events, and `IntrospectionError` for
  full-database scan orchestration.
- Added snapshot persistence helpers and the canonical `svg + sigil + meta`
  artifact set.
- Replaced the `atlas scan` placeholder with a working command that resolves
  config, supports privacy/style/layout overrides, and writes artifacts safely.
- Added `PanelBuilder` and `AtlasLocalServer` so `atlas open` serves inline SVG
  over `http://localhost` and renders a searchable schema/table side panel.
- Replaced the `atlas info` placeholder with a selective metadata command and
  dedicated text/JSON/YAML formatters.
- Added focused unit and integration suites for 5A, 5B, and 5C, and wired
  Phase 5 into `tests/run_tests.sh` plus the new `make test-cli` target.
- Updated the historical Phase 0 packaging expectation so `scan`, `open`, and
  `info` are no longer treated as placeholders.

## Notes

- Two Phase 5B integration checks are intentionally skipped when the execution
  environment forbids binding a local HTTP port.
- `atlas info` defaults an unqualified table reference to schema `public`,
  which matches the normative Phase 5 contract even on engines whose natural
  default schema differs.
