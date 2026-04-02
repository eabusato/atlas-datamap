# Publishing Guide

## Scope

Phase 14C prepares Atlas Datamap for release distribution. The steps below only
describe how to build and validate artifacts locally and how the repository
workflow publishes from a semantic tag. They do not require publishing during
normal development.

## Build local distributions

```bash
source .venv312/bin/activate
python -m build
```

Expected outputs:

- `dist/atlas_datamap-1.0.0.tar.gz`
- `dist/atlas_datamap-1.0.0-<platform>.whl`

## Build platform wheels with cibuildwheel

```bash
source .venv312/bin/activate
python -m cibuildwheel --output-dir dist/wheels
```

The project keeps the native Sigilo build in `setup.py`, so wheel production
reuses the same C build helper as editable installs.

## Validate an installed wheel

```bash
source .venv312/bin/activate
python -m pip install --force-reinstall --no-deps dist/atlas_datamap-1.0.0-*.whl
python -c "import atlas; print(atlas.__version__)"
python -m atlas --help
```

## Release flow

1. Ensure `bash tests/run_tests.sh`, `ruff`, and `mypy` are green.
2. Update `CHANGELOG.md` and affected docs/specs.
3. Create a semantic Git tag such as `v1.0.0`.
4. Push the tag to trigger `.github/workflows/publish.yml`.
5. The workflow builds wheels and sdist, uploads artifacts, publishes to PyPI
   through Trusted Publishing, and generates API documentation with `pdoc`.

## Fallback behavior

If the native Sigilo library cannot be compiled during installation, `setup.py`
emits a warning and Atlas falls back to the Python renderer. The package
remains functional for scan, analysis, export, enrichment, QA, snapshots, and
offline sigilo generation, but large sigilo renders may be slower than the
native path.
