# Atlas Onboarding Manual

## Purpose

`atlas onboard` is the guided local workflow for setting up and running a full
Atlas pipeline against a real database. It is the recommended first command for
most users evaluating Atlas from GitHub or PyPI.

Primary command:

```bash
atlas onboard
```

Resume a previously saved manifest without re-answering the wizard:

```bash
atlas onboard --resume /path/to/atlas.onboard.json
```

The onboarding flow is intentionally local-first:

- database traffic goes only to the database you configure
- AI traffic is restricted to local endpoints such as `localhost` and `127.0.0.1`
- secrets can be written to a local `.env` file or read from an existing one
- Atlas does not upload credentials, snapshots, or schema metadata to third parties on its own

Important scope note:

- this stronger local-only AI boundary is a property of `atlas onboard`
- if an operator later runs `atlas enrich` or `atlas ask` with a manually
  authored AI config, the trust boundary becomes the endpoint configured there
- `masked` remains a name-based redaction mode, so `stats_only` or `no_samples`
  are still the better choices when sample-derived prompt context must be
  avoided

## What `atlas onboard` creates

On a successful run, the workflow creates:

- a local onboarding manifest: `atlas.onboard.json`
- a connection reference file: `atlas.connection.toml`
- an AI reference file: `atlas.ai.toml` when AI is enabled
- a selection file: `atlas.selection.json` when AI scope is limited
- a local env file when managed secrets are enabled
- scan artifacts, reports, exports, history snapshots, and optional semantic artifacts

The generated output tree is rooted in the workspace directory you choose at the
start of the wizard.

## Wizard flow

The wizard always starts with a privacy banner and then asks questions in this
order:

1. workspace and project metadata
2. sigilo rendering preferences
3. secret-handling mode
4. database connection questions
5. optional local-AI questions
6. env file path when secrets are involved
7. final confirmation to run the pipeline immediately

Some questions are conditional and appear only for certain engines or AI providers.

## Question-by-question reference

### Workspace directory for Atlas outputs

Prompt:

```text
Workspace directory for Atlas outputs
```

Default:

```text
./atlas_onboarding
```

What it does:

- defines the root directory for the manifest, reference files, env file, and generated outputs
- is expanded with `~` support before Atlas saves the manifest

### Project label

Prompt:

```text
Project label
```

Default:

```text
Atlas Onboarding Run
```

What it does:

- labels the run in the saved manifest
- appears in onboarding progress messages
- does not directly control output filenames

### Generated artifacts directory name

Prompt:

```text
Generated artifacts directory name
```

Default:

```text
generated
```

What it does:

- names the subdirectory under the workspace that receives scan, export, report, history, diff, and semantic outputs

### Sigilo style

Prompt:

```text
Sigilo style
```

Choices:

- `network`
- `seal`
- `compact`

Default:

```text
network
```

What it does:

- controls the visual preset passed to the sigilo renderer
- affects both the base scan sigilo and the semantic sigilo when AI enrichment is enabled

Practical guidance:

- `network`: the general-purpose default
- `seal`: a more stylized alternative
- `compact`: denser layout for tighter visual packing

### Sigilo layout

Prompt:

```text
Sigilo layout
```

Choices:

- `circular`
- `force`

Default:

```text
circular
```

What it does:

- controls the spatial layout passed to the datamap sigilo builder
- affects both the base and semantic sigilo renders

Practical guidance:

- `circular`: deterministic and easier to compare across runs
- `force`: more organic graph layout, often better for relationship-heavy maps

### Use a managed local .env file for any secrets entered in this wizard?

Prompt:

```text
Use a managed local .env file for any secrets entered in this wizard?
```

Default:

```text
yes
```

What it does:

- `yes`: Atlas asks for secret values inside the wizard and writes them to a local env file
- `no`: Atlas expects those values to exist already in an env file you point to later

Important behavior:

- if managed env mode is enabled, Atlas writes the env file before the final
  "run now?" confirmation
- if you choose not to run immediately, the manifest and managed env file are still saved

## Database section

### Database engine

Prompt:

```text
Database engine
```

Choices:

- `postgresql`
- `mysql`
- `mssql`
- `sqlite`
- `generic`

Default:

```text
sqlite
```

What it does:

- selects which branch of connection questions the wizard will ask
- controls which `AtlasConnectionConfig` path is built later

Notes:

- MariaDB uses the `mysql` engine path in this wizard
- `generic` means a SQLAlchemy URL supplied through an env var

### SQLite database path

Shown only when `Database engine = sqlite`.

Prompt:

```text
SQLite database path
```

Default:

```text
./database.sqlite
```

What it does:

- stores the path used as the SQLite database target
- also becomes the `database` value in the saved database setup

### Env var name that stores the SQLAlchemy URL

Shown only when `Database engine = generic`.

Prompt:

```text
Env var name that stores the SQLAlchemy URL
```

Default:

```text
ATLAS_DB_URL
```

What it does:

- tells Atlas which env var contains the full SQLAlchemy URL
- is written into the saved connection reference file as `url_env`

### Database SQLAlchemy URL

Shown only when:

- `Database engine = generic`
- managed env mode is enabled

Prompt:

```text
Database SQLAlchemy URL
```

Behavior:

- input is hidden
- value is written to the managed env file under the env var you named above

When managed env mode is disabled:

- the wizard does not ask for the URL value
- you must ensure the chosen env var already exists in the env file you later provide

### Env var name for the database user

Shown only when `Database engine` is `postgresql`, `mysql`, or `mssql`.

Prompt:

```text
Env var name for the database user
```

Default:

```text
ATLAS_DB_USER
```

What it does:

- stores the env-var name that Atlas will use to resolve the database username

### Env var name for the database password

Shown only when `Database engine` is `postgresql`, `mysql`, or `mssql`.

Prompt:

```text
Env var name for the database password
```

Default:

```text
ATLAS_DB_PASSWORD
```

What it does:

- stores the env-var name that Atlas will use to resolve the database password

### Database user

Shown only when:

- `Database engine` is `postgresql`, `mysql`, or `mssql`
- managed env mode is enabled

Prompt:

```text
Database user
```

What it does:

- writes the username value to the managed env file

### Database password

Shown only when:

- `Database engine` is `postgresql`, `mysql`, or `mssql`
- managed env mode is enabled

Prompt:

```text
Database password
```

Behavior:

- input is hidden
- writes the password value to the managed env file

When managed env mode is disabled:

- the wizard does not ask for user or password values
- Atlas expects both env vars to already exist in the env file you later provide

### Database host

Shown only when `Database engine` is `postgresql`, `mysql`, or `mssql`.

Prompt:

```text
Database host
```

Default:

```text
127.0.0.1
```

What it does:

- stores the hostname used by the connector

### Database port

Shown only when `Database engine` is `postgresql`, `mysql`, or `mssql`.

Prompt:

```text
Database port
```

Defaults by engine:

- PostgreSQL: `5432`
- MySQL: `3306`
- SQL Server: `1433`

What it does:

- stores the TCP port used by the connector

### Database name

Shown only when `Database engine` is `postgresql`, `mysql`, or `mssql`.

Prompt:

```text
Database name
```

What it does:

- stores the database name passed to the connection config

### SSL mode

Shown only when `Database engine` is `postgresql`, `mysql`, or `mssql`.

Prompt:

```text
SSL mode
```

Default:

```text
disable
```

What it does:

- stores the `ssl_mode` field in the connection config
- the exact meaning depends on the connector and driver you use

### Connection timeout (seconds)

Prompt:

```text
Connection timeout (seconds)
```

Default:

```text
30
```

What it does:

- sets `timeout_seconds` in the saved database setup
- applies to connection and inspection operations that honor the shared config timeout

### Sample limit per live query

Prompt:

```text
Sample limit per live query
```

Default:

```text
50
```

What it does:

- sets the maximum number of live sample rows Atlas will pull in operations that use sampling
- matters for privacy-sensitive and AI-related workflows

### Privacy mode

Prompt:

```text
Privacy mode
```

Choices:

- `normal`
- `masked`
- `stats_only`
- `no_samples`

Default:

```text
masked
```

What each mode means:

- `normal`: live samples may contain raw values
- `masked`: live samples are allowed, but sensitive-name columns are redacted
- `stats_only`: row samples are blocked, but aggregate statistics remain allowed
- `no_samples`: no live row sampling is allowed

### Schema include list (comma-separated, blank for all)

Prompt:

```text
Schema include list (comma-separated, blank for all)
```

Default:

- blank

What it does:

- stores a schema allowlist
- blank means Atlas does not restrict the run to a fixed schema subset

Format:

- comma-separated schema names
- example: `public,billing,audit`

### Schema exclude list (comma-separated)

Prompt:

```text
Schema exclude list (comma-separated)
```

Default:

- blank

What it does:

- stores a schema denylist
- is applied together with the include list and engine defaults

Format:

- comma-separated schema names
- example: `information_schema,pg_catalog`

## AI section

### Enable local AI enrichment?

Prompt:

```text
Enable local AI enrichment?
```

Default:

```text
no
```

What it does:

- `no`: the wizard skips all AI questions and runs only the structural pipeline
- `yes`: the wizard collects local-AI settings and enables semantic enrichment

### Local AI provider

Shown only when AI enrichment is enabled.

Prompt:

```text
Local AI provider
```

Choices:

- `ollama`
- `llamacpp`
- `openai_compatible`

Default:

```text
ollama
```

What it does:

- selects the provider configuration branch used to build `atlas.ai.toml`

### Ollama base URL

Shown only when `Local AI provider = ollama`.

Prompt:

```text
Ollama base URL
```

Default:

```text
http://127.0.0.1:11434
```

### Model name

Shown when the provider is `ollama` or `openai_compatible`.

Prompt:

```text
Model name
```

Default:

```text
qwen2.5:1.5b
```

### llama.cpp base URL

Shown only when `Local AI provider = llamacpp`.

Prompt:

```text
llama.cpp base URL
```

Default:

```text
http://127.0.0.1:8080
```

### Model label

Shown only when `Local AI provider = llamacpp`.

Prompt:

```text
Model label
```

Default:

```text
local-model
```

### OpenAI-compatible local base URL

Shown only when `Local AI provider = openai_compatible`.

Prompt:

```text
OpenAI-compatible local base URL
```

Default:

```text
http://127.0.0.1:8000
```

### Local-only base URL restriction

All AI provider URLs are validated to ensure the hostname is one of:

- `localhost`
- `127.0.0.1`
- `::1`

If you enter any other host, onboarding fails immediately with a local-only
validation error.

### AI temperature

Prompt:

```text
AI temperature
```

Default:

```text
0.1
```

What it does:

- sets the provider temperature in the generated AI config

### AI max tokens

Prompt:

```text
AI max tokens
```

Default:

```text
300
```

What it does:

- limits generated response size for semantic calls

### AI timeout (seconds)

Prompt:

```text
AI timeout (seconds)
```

Default:

```text
60.0
```

What it does:

- sets the provider request timeout

### Parallel AI table workers

Prompt:

```text
Parallel AI table workers
```

Default:

```text
2
```

What it does:

- controls table-level semantic enrichment concurrency

### Column analysis mode

Prompt:

```text
Column analysis mode
```

Choices:

- `infer`
- `full`
- `skip`

Default:

```text
infer
```

What each mode means:

- `infer`: derive column semantics heuristically without a full LLM call per column
- `full`: run full semantic enrichment for columns through the local model
- `skip`: enrich tables only and skip column semantics

Practical guidance:

- `infer`: best balance of speed and detail for first runs
- `full`: best semantic depth, but slower and more expensive locally
- `skip`: best when you only need table-level understanding

### Ignore semantic cache?

Prompt:

```text
Ignore semantic cache?
```

Default:

```text
no
```

What it does:

- `no`: reuse semantic cache entries when signatures match
- `yes`: force recomputation even when cached results exist

### Does this local gateway require an API key?

Shown only when `Local AI provider = openai_compatible`.

Prompt:

```text
Does this local gateway require an API key?
```

Default:

```text
no
```

What it does:

- controls whether Atlas asks for an env var name for the local gateway API key

### Env var name for the local AI API key

Shown only when:

- `Local AI provider = openai_compatible`
- API key confirmation is `yes`

Prompt:

```text
Env var name for the local AI API key
```

Default:

```text
ATLAS_AI_API_KEY
```

### API key

Shown only when:

- `Local AI provider = openai_compatible`
- API key confirmation is `yes`
- managed env mode is enabled

Prompt:

```text
API key
```

Behavior:

- input is hidden
- written to the managed env file

### Limit AI to schemas (comma-separated, blank for all)

Prompt:

```text
Limit AI to schemas (comma-separated, blank for all)
```

Default:

- blank

What it does:

- limits semantic enrichment to selected schemas only

### Limit AI to tables (schema.table, comma-separated)

Prompt:

```text
Limit AI to tables (schema.table, comma-separated)
```

Default:

- blank

What it does:

- limits semantic enrichment to specific fully-qualified tables

Format:

- `schema.table`
- example: `public.orders,billing.invoices`

### Limit AI to columns (schema.table.column, comma-separated)

Prompt:

```text
Limit AI to columns (schema.table.column, comma-separated)
```

Default:

- blank

What it does:

- limits column-level enrichment to specific fully-qualified columns

Format:

- `schema.table.column`
- example: `public.orders.status,public.orders.created_at`

## Env-file question

### Managed env file path

Shown only when:

- secrets are needed
- managed env mode is enabled

Prompt:

```text
Managed env file path
```

Default:

```text
.env
```

What it does:

- defines where Atlas will write the managed env file
- relative paths are resolved under the workspace directory

### Existing env file path

Shown only when:

- secrets are needed
- managed env mode is disabled

Prompt:

```text
Existing env file path
```

Default:

```text
.env
```

What it does:

- tells Atlas where to read existing secret values from
- relative paths are resolved under the workspace directory

Important:

- Atlas does not populate this file for you when managed env mode is disabled
- the required env vars must already exist there by the time you run the pipeline

## Final confirmation

### Run the full Atlas onboarding pipeline now?

Prompt:

```text
Run the full Atlas onboarding pipeline now?
```

Default:

```text
yes
```

What it does:

- `yes`: Atlas runs the full scan, render, export, report, history, diff, and optional semantic pipeline immediately
- `no`: Atlas saves the manifest and any managed env file, then exits without running the pipeline

This is useful if you want to inspect or edit the generated reference files before running.

## Files and directories written by a successful run

### Workspace root

Typical files:

- `atlas.onboard.json`
- `atlas.connection.toml`
- `atlas.ai.toml` when AI is enabled
- `atlas.selection.json` when AI scope is restricted
- `.env` or your chosen env file path when managed secrets are enabled

### Generated output tree

Under the generated artifacts directory, Atlas writes:

- `scans/`
- `exports/`
- `reports/`
- `history/`
- `diff/`
- `semantic/` when AI enrichment is enabled

### Base scan artifacts

Under `scans/`, Atlas writes:

- `{stem}.svg`
- `{stem}.sigil`
- `{stem}_meta.json`
- `{stem}_panel.html`
- `{stem}.atlas`

### Export artifacts

Under `exports/`, Atlas writes:

- `{stem}_standalone.html`
- `dictionary.json`
- `tables.csv`
- `columns.csv`
- `dictionary.md`

### Reports

Under `reports/`, Atlas writes:

- `{stem}_health_report.html`
- `{stem}_executive_report.html`

### Diff output

Under `diff/`, Atlas writes:

- `{stem}_diff.html` when a previous history snapshot already exists

If no previous snapshot exists, onboarding skips diff generation and reports that explicitly.

### History

Under `history/`, Atlas writes:

- the current snapshot with a generated history name
- future runs compare against the latest previous snapshot in this directory

### Semantic artifacts

When AI is enabled, under `semantic/`, Atlas writes:

- `{stem}_semantic.svg`
- `{stem}_semantic.sigil`
- `{stem}_semantic_meta.json`
- `{stem}_semantic_panel.html`
- `{stem}_semantic.atlas`

## Resume mode

If you already have an onboarding manifest, you can rerun the pipeline without
re-answering the wizard:

```bash
atlas onboard --resume ./atlas_onboarding/atlas.onboard.json
```

Resume mode:

- loads the saved manifest
- reuses the stored workspace, database, AI, and env-file references
- reruns the full pipeline immediately
- prints the resulting output summary as JSON

## Recommended first-run choices

For most first runs:

- engine: your real target engine, or `sqlite` for a local test database
- privacy mode: `masked`
- sigilo style: `network`
- sigilo layout: `circular`
- AI enrichment: `no` on the first structural pass, then `yes` once the base scan is working
- column mode: `infer` for the first semantic run
- managed env mode: `yes` unless you already maintain an existing env file

## Related docs

- [Getting Started](../getting_started.md)
- [System Manual](system_manual.md)
- [Privacy Modes](../privacy.md)
- [Full Product Showcase](../full_product_showcase.md)
