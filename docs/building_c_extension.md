# Building the C Extension

`atlas.sigilo` uses a vendored shared library named `libatlas_sigilo` for the
native layout and SVG rendering path. Atlas keeps a pure-Python fallback, but
the native build is the canonical Phase 3 implementation.

## Requirements

- Python 3.11 or 3.12
- `cffi`
- one C toolchain:
  - `cmake` + `gcc` or `clang`, preferred
  - `make` + `cc`, fallback

## Quick build

```bash
make build-c
python scripts/check_sigilo_build.py
```

If `cmake` is not installed, Atlas falls back to the vendored
[`atlas/_c/Makefile`](../atlas/_c/Makefile).

## Focused validation

```bash
make test-c-smoke
make test-sigilo
```

## Packaging flow

Editable and wheel builds run the same helper in
[`atlas/_c/build_lib.py`](../atlas/_c/build_lib.py). The
custom `build_ext` in [`setup.py`](../setup.py):

- tries `cmake` first
- falls back to `make`
- copies `libatlas_sigilo.*` into the package directory
- warns instead of aborting when no compiler toolchain is available

When the native build fails, `atlas.sigilo` still works through the Python
fallback renderer.

## Vendored sources

Atlas vendors the standalone CCT files under:

- [`atlas/_c/common/`](../atlas/_c/common)
- [`atlas/_c/sigil/`](../atlas/_c/sigil)

The shared library also includes Atlas-owned sources:

- [`atlas/_c/atlas_render.c`](../atlas/_c/atlas_render.c)
- [`atlas/_c/atlas_sigilo.c`](../atlas/_c/atlas_sigilo.c)

## Operational limits

- The native renderer currently targets Unix-like toolchains in the validated
  local workflow. The package includes `.dll` packaging hooks, but Windows
  build validation is not covered by this phase's local regression suite.
- The fallback renderer keeps the same SVG data attributes and metadata
  structure, but its layout is grid-based rather than the native circular
  schema layout.
