# Full Product Showcase

## Purpose

This showcase exercises the Atlas product end to end against one reproducible,
fictional database with enough structure to stress the current Step 1 feature
set.

Atlas Datamap is the standalone Python product derived from the broader sigilo
work first developed in the CCT project:

- CCT: [`github.com/eabusato/cct`](https://github.com/eabusato/cct)
- Atlas Datamap: [`github.com/eabusato/atlas-datamap`](https://github.com/eabusato/atlas-datamap)

The generated bundle covers:

- scanning and sigilo generation
- panel/open-ready HTML
- table inspection
- textual search
- heuristic discovery
- health and executive reporting
- structured exports
- semantic enrichment
- natural-language QA
- snapshot packaging
- offline diffing
- local history archives
- public SDK usage

The showcase does not change the approved sigilo visual formulation. It only
feeds that renderer with a larger, more realistic database.

## Dataset

The fixture models a fictional financial-commerce platform called `aurora_demo`.

Business areas represented in the schema:

- CRM and customer master data
- accounts, cards, devices, and sessions
- merchants and merchant locations
- orders, invoices, payments, refunds
- ledger accounts and ledger entries
- risk cases and risk alerts
- support tickets and support messages
- compliance reviews
- FX rates, balances, config, and import batches

Version `v1` contains:

- `33` tables
- `3` views
- `221` columns
- one SQLite schema: `main`

Version `v2` evolves the model with structural changes used by the diff flow:

- adds `main.chargebacks`
- adds `main.vw_chargeback_exposure`
- removes `main.marketing_events`
- adds `2` new relationships and removes `1`

## Source Files

- Generator: [`../examples/full_showcase/build_full_showcase.py`](../examples/full_showcase/build_full_showcase.py)
- Output root: [`../examples/full_showcase/generated`](../examples/full_showcase/generated)
- Base config: [`../examples/full_showcase/generated/atlas.toml`](../examples/full_showcase/generated/atlas.toml)
- Ollama config template: [`../examples/full_showcase/generated/atlas.ai.ollama.toml`](../examples/full_showcase/generated/atlas.ai.ollama.toml)
- Bundle manifest: [`../examples/full_showcase/generated/showcase_manifest.json`](../examples/full_showcase/generated/showcase_manifest.json)

## Guided real-world equivalent

The showcase is now mirrored by a guided product path for real databases:

```bash
atlas onboard
```

That interactive flow asks for the live database settings, privacy mode, sigilo
preferences, local-AI settings, and optional local `.env` storage. It then runs
the same category of outputs that this showcase demonstrates, but against the
user's own database.

## Regeneration

Generate the full bundle from scratch:

```bash
source .venv312/bin/activate
python examples/full_showcase/build_full_showcase.py
```

This base run writes the structural bundle under:

- [`../examples/full_showcase/generated`](../examples/full_showcase/generated)

To generate the semantic and QA bundle with a real local model:

```bash
source .venv312/bin/activate
python examples/full_showcase/build_full_showcase.py --enable-ollama
```

By default, the Ollama stage uses:

- model: `qwen2.5:1.5b`
- base URL: `http://127.0.0.1:11434`

That model is small enough to be practical on an M1 Pro while still being more
likely to follow Atlas' structured JSON prompts than ultra-tiny toy models.

## Generated Artifacts

Base scan artifacts:

- [`../examples/full_showcase/generated/scans/aurora_demo_v1.svg`](../examples/full_showcase/generated/scans/aurora_demo_v1.svg)
- [`../examples/full_showcase/generated/scans/aurora_demo_v1.sigil`](../examples/full_showcase/generated/scans/aurora_demo_v1.sigil)
- [`../examples/full_showcase/generated/scans/aurora_demo_v1_meta.json`](../examples/full_showcase/generated/scans/aurora_demo_v1_meta.json)
- [`../examples/full_showcase/generated/scans/aurora_demo_v1.atlas`](../examples/full_showcase/generated/scans/aurora_demo_v1.atlas)
- [`../examples/full_showcase/generated/scans/aurora_demo_v1_panel.html`](../examples/full_showcase/generated/scans/aurora_demo_v1_panel.html)

Reports and exports:

- [`../examples/full_showcase/generated/reports/aurora_health_report.html`](../examples/full_showcase/generated/reports/aurora_health_report.html)
- [`../examples/full_showcase/generated/reports/aurora_executive_report.html`](../examples/full_showcase/generated/reports/aurora_executive_report.html)
- [`../examples/full_showcase/generated/exports/aurora_demo_v1_standalone.html`](../examples/full_showcase/generated/exports/aurora_demo_v1_standalone.html)
- [`../examples/full_showcase/generated/exports/dictionary.json`](../examples/full_showcase/generated/exports/dictionary.json)
- [`../examples/full_showcase/generated/exports/tables.csv`](../examples/full_showcase/generated/exports/tables.csv)
- [`../examples/full_showcase/generated/exports/columns.csv`](../examples/full_showcase/generated/exports/columns.csv)
- [`../examples/full_showcase/generated/exports/dictionary.md`](../examples/full_showcase/generated/exports/dictionary.md)

Search and inspection samples:

- [`../examples/full_showcase/generated/queries/search_payment_dispute.txt`](../examples/full_showcase/generated/queries/search_payment_dispute.txt)
- [`../examples/full_showcase/generated/queries/discovery_risk_alerts.json`](../examples/full_showcase/generated/queries/discovery_risk_alerts.json)
- [`../examples/full_showcase/generated/queries/info_payments.json`](../examples/full_showcase/generated/queries/info_payments.json)

Snapshot evolution:

- [`../examples/full_showcase/generated/diff/aurora_demo_v2.atlas`](../examples/full_showcase/generated/diff/aurora_demo_v2.atlas)
- [`../examples/full_showcase/generated/diff/aurora_demo_diff.html`](../examples/full_showcase/generated/diff/aurora_demo_diff.html)
- [`../examples/full_showcase/generated/history`](../examples/full_showcase/generated/history)

Ollama-only semantic artifacts:

These files appear only after running the showcase with `--enable-ollama`:

- `generated/semantic/aurora_demo_v1_semantic.svg`
- `generated/semantic/aurora_demo_v1_semantic.sigil`
- `generated/semantic/aurora_demo_v1_semantic.atlas`
- `generated/semantic/aurora_demo_v1_semantic_standalone.html`
- `generated/semantic/aurora_demo_v1_semantic_executive.html`
- `generated/semantic/exports/dictionary.json`
- `generated/semantic/exports/tables.csv`
- `generated/semantic/exports/columns.csv`
- `generated/semantic/exports/dictionary.md`
- `generated/semantic/ask_payment_disputes.json`
- `generated/semantic/ask_payment_disputes.txt`

## CLI Walkthrough

### 1. Scan

```bash
atlas scan --config examples/full_showcase/generated/atlas.toml --output examples/full_showcase/generated/scans
```

What it demonstrates:

- live introspection
- canonical `.svg`, `.sigil`, and `_meta.json` outputs
- sigilo rendering on a dense graph

### 2. Open

```bash
atlas open examples/full_showcase/generated/scans/aurora_demo_v1.svg
```

What it demonstrates:

- local browser serving with hover and side panel
- same SVG that the rest of the showcase reuses

The bundle already includes an offline HTML equivalent in:

- [`../examples/full_showcase/generated/scans/aurora_demo_v1_panel.html`](../examples/full_showcase/generated/scans/aurora_demo_v1_panel.html)

### 3. Info

```bash
atlas info --config examples/full_showcase/generated/atlas.toml --table main.payments --format json
```

Output sample:

- [`../examples/full_showcase/generated/queries/info_payments.json`](../examples/full_showcase/generated/queries/info_payments.json)

### 4. Search

```bash
atlas search --config examples/full_showcase/generated/atlas.toml "payment dispute"
```

Output sample:

- [`../examples/full_showcase/generated/queries/search_payment_dispute.txt`](../examples/full_showcase/generated/queries/search_payment_dispute.txt)

### 5. Heuristic Discovery

Discovery is a library workflow rather than a first-class CLI command in the
current product surface.

Saved result:

- [`../examples/full_showcase/generated/queries/discovery_risk_alerts.json`](../examples/full_showcase/generated/queries/discovery_risk_alerts.json)

This example intentionally uses the question `where are risk alerts tracked?`
because it shows the Phase 7 heuristic layer in a clean, deterministic way.

### 6. Health Report

```bash
atlas report --config examples/full_showcase/generated/atlas.toml --output examples/full_showcase/generated/reports/aurora_health_report.html
```

Generated file:

- [`../examples/full_showcase/generated/reports/aurora_health_report.html`](../examples/full_showcase/generated/reports/aurora_health_report.html)

### 7. Executive Report

```bash
atlas report --atlas examples/full_showcase/generated/scans/aurora_demo_v1.atlas --style executive --output examples/full_showcase/generated/reports/aurora_executive_report.html
```

Generated file:

- [`../examples/full_showcase/generated/reports/aurora_executive_report.html`](../examples/full_showcase/generated/reports/aurora_executive_report.html)

### 8. Structured Export

```bash
atlas export json --atlas examples/full_showcase/generated/scans/aurora_demo_v1.atlas --output examples/full_showcase/generated/exports/dictionary.json
atlas export csv --atlas examples/full_showcase/generated/scans/aurora_demo_v1.atlas --entity tables --output examples/full_showcase/generated/exports/tables.csv
atlas export csv --atlas examples/full_showcase/generated/scans/aurora_demo_v1.atlas --entity columns --output examples/full_showcase/generated/exports/columns.csv
atlas export markdown --atlas examples/full_showcase/generated/scans/aurora_demo_v1.atlas --output examples/full_showcase/generated/exports/dictionary.md
atlas export svg --atlas examples/full_showcase/generated/scans/aurora_demo_v1.atlas --output examples/full_showcase/generated/exports/aurora_demo_v1_standalone.html
```

### 9. Semantic Enrichment

Real product path with Ollama:

```bash
ollama pull qwen2.5:1.5b
python examples/full_showcase/build_full_showcase.py --enable-ollama
```

Equivalent direct Atlas CLI command:

```bash
atlas enrich --ai-config examples/full_showcase/generated/atlas.ai.ollama.toml --sigil examples/full_showcase/generated/scans/aurora_demo_v1.sigil --output examples/full_showcase/generated/semantic
```

Semantic highlights after the Ollama stage runs:

- semantic snapshot with table and column semantics
- semantic SVG hover payloads
- semantic standalone HTML
- semantic executive report

### 10. Natural-Language QA

Real product path with Ollama:

```bash
atlas ask --ai-config examples/full_showcase/generated/atlas.ai.ollama.toml --sigil examples/full_showcase/generated/semantic/aurora_demo_v1_semantic.sigil "Where are payment disputes tracked?"
```

Saved example answer appears after the Ollama stage:

- `generated/semantic/ask_payment_disputes.txt`

Current interpretation status:

- Atlas asks about metadata, not about the SVG as an image
- it works best after `enrich`
- it degrades gracefully when embeddings are unavailable
- once Ollama is configured, this exact showcase runs against a real local model

### 11. Snapshot Diff

```bash
atlas diff examples/full_showcase/generated/scans/aurora_demo_v1.atlas examples/full_showcase/generated/diff/aurora_demo_v2.atlas --output examples/full_showcase/generated/diff/aurora_demo_diff.html
```

Observed structural delta in the prepared showcase:

- added: `main.chargebacks`
- added: `main.vw_chargeback_exposure`
- removed: `main.marketing_events`
- new relations: `2`
- removed relations: `1`

### 12. History

```bash
atlas history list --dir examples/full_showcase/generated/history
atlas history diff --dir examples/full_showcase/generated/history --from latest --to 20260402 --output examples/full_showcase/generated/history/history_diff.html
atlas history open --dir examples/full_showcase/generated/history --date latest
```

Prepared history directory:

- [`../examples/full_showcase/generated/history`](../examples/full_showcase/generated/history)

The base showcase history contains two snapshots:

- base structural snapshot
- evolved v2 snapshot

After `--enable-ollama`, a third semantic snapshot is appended.

## Public SDK Walkthrough

The generator script also serves as the canonical programmatic example.

Core flow:

```python
from atlas import Atlas, AtlasConnectionConfig

atlas = Atlas(AtlasConnectionConfig.from_url("sqlite:////absolute/path/to/aurora_demo_v1.db"))
result = atlas.scan()
sigilo = atlas.build_sigilo(result, style="network", layout="circular")
snapshot = atlas.create_snapshot(result, sigilo)
snapshot.save("aurora_demo_v1.atlas")
```

The same script then layers:

- `atlas.save_scan_artifacts(...)`
- diff/history packaging via `AtlasSnapshot` and `AtlasHistory`

With `--enable-ollama`, the same script additionally layers:

- semantic enrichment through `atlas.enrich(...)`
- natural-language QA through `atlas.ask(...)`

## AI Notes For Real Local Models

- install and start Ollama
- pull a small model
- rerun the showcase with `--enable-ollama`

Minimal example:

```toml
[ai]
provider = "ollama"
model = "qwen2.5:1.5b"
base_url = "http://127.0.0.1:11434"
temperature = 0.1
max_tokens = 300
timeout_seconds = 60.0
```

Recommended first command on your machine:

```bash
ollama pull qwen2.5:1.5b
```

## What This Showcase Proves

- Atlas can scan and visualize a non-trivial database locally
- the sigilo remains usable with dozens of tables and hundreds of columns
- the product surface is coherent across CLI, snapshots, reports, exports, and SDK
- semantic enrichment and QA now have a real local Ollama path in the showcase
- the current Step 1 implementation is demonstrable without external SaaS dependencies
