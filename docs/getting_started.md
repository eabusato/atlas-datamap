# Getting Started

## Project lineage

Atlas Datamap is the database-mapping product that emerged from the broader
CCT language project. CCT lives at
[`github.com/eabusato/cct`](https://github.com/eabusato/cct), while Atlas
Datamap lives at
[`github.com/eabusato/atlas-datamap`](https://github.com/eabusato/atlas-datamap).

Atlas reuses architectural ideas and sigilo visual language from that work, but
it is a standalone Python package and CLI.

## Requirements

- Python 3.11 or newer
- `make`
- Docker Desktop for Phase 1 PostgreSQL integration tests

## Development bootstrap

```bash
/opt/homebrew/bin/python3.12 -m venv .venv312
source .venv312/bin/activate
python -m pip install -e ".[dev]"
```

## Core commands

```bash
atlas --help
atlas --version
python -m atlas --help
make lint
make typecheck
tests/run_tests.sh
```

## Current scope

- Atlas Step 1 is implemented end to end.
- The CLI surface includes `scan`, `open`, `info`, `search`, `report`,
  `onboard`, `export`, `enrich`, `ask`, `diff`, and `history`.
- The Python SDK also exposes the public `Atlas` facade for programmatic use.
- The full reproducible product walkthrough lives in
  [`full_product_showcase.md`](full_product_showcase.md).

## Guided onboarding

For a first real run against your own database, prefer the interactive wizard:

```bash
atlas onboard
```

The wizard asks for:

- database engine and connection details
- privacy mode and sampling limits
- sigilo style and layout
- optional local AI settings
- optional `.env` handling for local-only secrets

At the end, Atlas runs the complete local pipeline and writes a workspace with:

- scan artifacts (`.svg`, `.sigil`, `_meta.json`, `.atlas`)
- panel and standalone HTML
- health and executive reports
- JSON, CSV, and Markdown exports
- snapshot history
- a diff report when a previous local snapshot already exists
- optional semantic outputs when local AI is enabled

The onboarding flow keeps secrets in local files only. It does not upload
credentials, metadata, or snapshots to third parties on its own. Database
traffic goes only to the configured database, and onboarding restricts AI
providers to local endpoints so semantic prompts remain on the user's machine.
