# Phase 14 Contracts

## Public SDK contract

Phase 14A introduces the stable facade in
[`atlas/sdk.py`](../../atlas/sdk.py).

Public entry points:

- `Atlas`
- `AtlasSigiloArtifact`

Operational rules:

- `Atlas.scan()` delegates to `IntrospectionRunner`
- `Atlas.build_sigilo()` delegates to `DatamapSigiloBuilder`
- `Atlas.save_scan_artifacts()` preserves the canonical `.svg`, `.sigil`, and
  `_meta.json` outputs
- `Atlas.create_snapshot()` delegates to `AtlasSnapshot.from_result(...)`
- `Atlas.enrich()` mutates the provided `IntrospectionResult` in place and
  preserves the Phase 8/9 semantic contracts
- `Atlas.ask()` preserves the Phase 10 QA contract and can operate without a
  persisted vector index
- `Atlas.detect_local_llm()` raises the stable error
  `"No local LLM provider is reachable."` when autodetection cannot reach a
  local provider

Artifact contract:

- `AtlasSigiloArtifact.save()` accepts either a target directory or a full file path
- `AtlasSigiloArtifact.to_svg_text()` returns a UTF-8 decoded SVG string

## Regression and visual-baseline contract

Phase 14B adds the baseline helper in
[`tests/support/svg_baseline.py`](../../tests/support/svg_baseline.py)
and the approved SVG baselines under
[`tests/baselines/phase_14/`](../../tests/baselines/phase_14).

Normalization rules:

- build comments may be removed
- absolute database-file paths may be normalized
- tooltip ids may be normalized

Protected visual/semantic surface:

- SVG geometry and node placement
- visible labels
- structural CSS classes
- `data-*` attributes used by hover, semantics, and public sigilo contracts

Privacy regression rules:

- `masked` still redacts sensitive sample values
- `stats_only` must not request live sample rows during semantic enrichment
- `no_samples` must still block direct sampling with `PrivacyViolationError`

Runner and CI rules:

- `tests/run_tests.sh` is the single historical runner
- Phase 14A, 14B, and 14C are integrated into the same coordinator
- `.github/workflows/build.yml` runs the full regression suite on Linux and a
  portable subset on macOS

## Distribution contract

Phase 14C promotes the package to the Step 1 release surface.

Versioning rules:

- `atlas/version.py` and `pyproject.toml` both expose `1.0.0`
- `atlas --version` remains driven by `ATLAS_VERSION`

Packaging rules:

- package name remains `atlas-datamap`
- extras remain optional: `postgresql`, `mysql`, `mssql`, `generic`, `ai`,
  `sigilo`, and `dev`
- `py.typed` remains packaged
- the native Sigilo build still flows through `setup.py`
- the wheel should include the native Sigilo artifact when local build succeeds
- the package remains functional with the Python fallback when native build is
  unavailable

Source-distribution rules:

- `MANIFEST.in` includes `CHANGELOG.md`
- `MANIFEST.in` includes `docs/publishing.md`
- the vendored C sources remain present in the source distribution

Release automation rules:

- `.github/workflows/publish.yml` is tag-driven
- wheel and sdist artifacts are uploaded before PyPI publication
- API docs are generated with `pdoc`
