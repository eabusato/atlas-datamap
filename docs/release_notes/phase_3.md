# Phase 3 Release Notes

## Added

- Vendored standalone CCT sigilo/common sources under
  [`atlas/_c/`](../../atlas/_c).
- New Atlas-native SVG renderer in
  [`atlas/_c/atlas_render.c`](../../atlas/_c/atlas_render.c).
- ABI-level `cffi` wrapper in
  [`atlas/_sigilo.py`](../../atlas/_sigilo.py).
- High-level `SigiloBuilder`, `SigiloNode`, `SigiloEdge`, `SigiloConfig`, and
  Python fallback renderer under
  [`atlas/sigilo/`](../../atlas/sigilo).
- Native build helper, packaging hooks, root Makefile targets, CI workflow, and
  focused build documentation for the sigilo library.

## Verified

- Standalone compilation of `libatlas_sigilo` through the vendored Makefile.
- Dynamic loading of the shared library through `cffi.dlopen()`.
- Native `IntrospectionResult -> SVG` rendering through `SigiloBuilder`.
- Python fallback rendering when the native library is unavailable or fails at
  runtime.
- End-to-end build checks through `setup.py build_ext --inplace`,
  `make build-c`, and `scripts/check_sigilo_build.py`.

## Notes

- The repository-local `.venv` still points to Python 3.10 and remains outside
  the supported runtime contract; the validated environment continues to be
  `.venv312`.
- Native build validation in this block uses Unix-like toolchains. Windows
  packaging hooks exist but were not exercised by the local regression suite.
