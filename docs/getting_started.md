# Getting Started

## First command

If you want the fastest path to a real Atlas run, start here:

```bash
atlas onboard
```

`atlas onboard` is the guided workflow that asks for connection details,
privacy mode, sigilo settings, optional local-AI settings, and local secret
handling. It then creates a local workspace and runs the full Atlas pipeline
for you.

Question-by-question reference:

- [`manuals/onboarding_manual.md`](manuals/onboarding_manual.md)

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
- Docker Desktop for integration tests that exercise real services
- a local AI runtime such as Ollama only if you want to use `atlas enrich` or `atlas ask`

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

The phase terminology used elsewhere in the repository is historical delivery
context. For day-to-day use, treat the current CLI and SDK surface as the
reference.

If you are reading this page for the first time, prefer `atlas onboard` first
and return to the lower-level commands after the guided run.

## Guided onboarding

For a first real run against your own database, prefer the interactive wizard:

```bash
atlas onboard
```

Complete wizard reference:

- [`manuals/onboarding_manual.md`](manuals/onboarding_manual.md)

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

Practical note:

- `masked` is a useful redaction mode, but it is still name-based
- if you need to avoid sample-derived prompt context, prefer `stats_only` or
  `no_samples`
- generated Atlas artifacts remain local, but they can contain rich schema
  metadata and should be reviewed before being shared

See also:

- [`privacy.md`](privacy.md)
