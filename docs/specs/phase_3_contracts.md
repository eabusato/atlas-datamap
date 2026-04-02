# Phase 3 Contracts

## Native sigilo contract

Phase 3 vendors a standalone subset of CCT into
[`atlas/_c/`](../../atlas/_c). The build produces
`libatlas_sigilo.so`, `libatlas_sigilo.dylib`, or `libatlas_sigilo.dll`
directly inside the Python package directory.

Vendored sources in this phase:

- [`atlas/_c/common/types.h`](../../atlas/_c/common/types.h)
- [`atlas/_c/common/errors.h`](../../atlas/_c/common/errors.h)
- [`atlas/_c/common/errors.c`](../../atlas/_c/common/errors.c)
- [`atlas/_c/common/diagnostic.h`](../../atlas/_c/common/diagnostic.h)
- [`atlas/_c/common/diagnostic.c`](../../atlas/_c/common/diagnostic.c)
- [`atlas/_c/sigil/sigil_parse.h`](../../atlas/_c/sigil/sigil_parse.h)
- [`atlas/_c/sigil/sigil_parse.c`](../../atlas/_c/sigil/sigil_parse.c)
- [`atlas/_c/sigil/sigil_validate.h`](../../atlas/_c/sigil/sigil_validate.h)
- [`atlas/_c/sigil/sigil_validate.c`](../../atlas/_c/sigil/sigil_validate.c)

Atlas-owned native sources:

- [`atlas/_c/atlas_render.h`](../../atlas/_c/atlas_render.h)
- [`atlas/_c/atlas_render.c`](../../atlas/_c/atlas_render.c)
- [`atlas/_c/atlas_sigilo.h`](../../atlas/_c/atlas_sigilo.h)
- [`atlas/_c/atlas_sigilo.c`](../../atlas/_c/atlas_sigilo.c)

The exported ABI required by the Python layer is:

- `atlas_sigilo_ping() -> const char *`
- `atlas_sigilo_abi_version() -> const char *`
- `atlas_render_version() -> const char *`
- `atlas_render_init()`
- `atlas_render_add_node()`
- `atlas_render_add_edge()`
- `atlas_render_compute_layout()`
- `atlas_render_svg_to_buffer()`
- `atlas_render_dispose()`

## Python sigilo contract

The Python entry points live in:

- [`atlas/_sigilo.py`](../../atlas/_sigilo.py)
- [`atlas/sigilo/types.py`](../../atlas/sigilo/types.py)
- [`atlas/sigilo/builder.py`](../../atlas/sigilo/builder.py)
- [`atlas/sigilo/_python_fallback.py`](../../atlas/sigilo/_python_fallback.py)

Runtime behavior:

- `atlas._sigilo.available()` reports whether the shared library was loaded
- `atlas._sigilo.ping()` returns the ABI string from the shared library
- `atlas._sigilo.RenderContext` owns a native `atlas_render_ctx_t`
- `SigiloBuilder.build_svg()` prefers the native renderer when available
- `SigiloBuilder.build_svg()` falls back to the Python renderer when the native
  library is missing or raises a runtime error

## Data mapping contract

`SigiloBuilder` converts `IntrospectionResult` into:

- `SigiloNode.id = "<schema>.<table>"`
- `SigiloNode.node_type` mapped from `TableType`
- `SigiloEdge.edge_type = "declared"` unless the foreign key is inferred

Each rendered node exposes, when enabled:

- `data-table`
- `data-schema`
- `data-row-estimate`
- `data-size-bytes`
- `data-column-count`
- `data-table-type`
- `data-comment` when present

Each rendered foreign-key edge exposes, when enabled:

- `data-fk-from`
- `data-fk-to`
- `data-fk-columns` for mapped source and target columns
- `data-fk-type`

SVG `<title>` metadata mirrors the same operational summary so hover-capable
consumers can inspect node and edge context without parsing the `data-*`
attributes.

## Build and packaging contract

The build orchestration lives in:

- [`setup.py`](../../setup.py)
- [`atlas/_c/build_lib.py`](../../atlas/_c/build_lib.py)
- [`Makefile`](../../Makefile)
- [`scripts/check_sigilo_build.py`](../../scripts/check_sigilo_build.py)

Implemented behavior:

- `make build-c` tries CMake first and falls back to Makefile
- `make clean-c` removes the packaged shared library and `_c/build/`
- `setup.py build_ext --inplace` runs the same build helper
- installation warnings are non-fatal; the package remains usable through the
  Python fallback

## Operational limits

- The native renderer uses the circular schema layout from Phase 3A; the
  Python fallback keeps the metadata contract but uses a simpler grid layout.
- Local validation in this repository covers macOS and Unix-like toolchains.
  Packaging hooks for `.dll` are present, but Windows runtime validation is
  not part of this phase's executed regression block.
- The vendored CCT subset is intentionally limited to standalone sigilo parse,
  validate, and common primitives. It does not embed the full CCT compiler or
  any `parser/ast.h` dependency chain.
