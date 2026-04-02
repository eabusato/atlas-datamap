# Phase 4 Release Notes

## Scope

Phase 4 adds the database-datamap layer on top of the Phase 3 sigilo
foundation:

- `DatamapSigiloBuilder` and style presets
- embedded hover metadata via inline JavaScript
- native force-directed layout in the C renderer and Python/CFFI bridge

## Delivered

- Added `SigiloStyle`, `StyleParams`, and style-driven canvas/ring defaults.
- Added `DatamapSigiloBuilder` with schema filtering, size scaling, style
  selection, circular/force layout selection, and force-layout parameter
  tuning.
- Expanded `SigiloNode`, `SigiloConfig`, native ABI structs, and SVG data
  attributes to include FK counts, index counts, precomputed radii, and
  schema-aware rendering parameters.
- Switched SVG wrapper classes to `system-node-wrap` and `system-edge-wrap`.
- Added embedded hover script generation and SVG injection through
  `HoverScriptBuilder`.
- Implemented native force-directed layout with schema cohesion and center
  attraction, plus the Python binding entry point
  `RenderContext.compute_layout_force()`.
- Added focused unit and integration suites for 4A, 4B, and 4C, and wired the
  new phases into `tests/run_tests.sh`.

## Notes

- Interactive hover depends on SVG viewers that allow embedded JavaScript.
- The Python fallback preserves the Phase 4 SVG contract and a deterministic
  force-style relaxation, but the full force-directed layout remains native-C
  optimized.
