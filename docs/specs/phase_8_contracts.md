# Phase 8 Contracts

## AI package contract

Phase 8 adds the local semantic-enrichment surface:

- [`atlas/ai/types.py`](../../atlas/ai/types.py)
- [`atlas/ai/client.py`](../../atlas/ai/client.py)
- [`atlas/ai/sampler.py`](../../atlas/ai/sampler.py)
- [`atlas/ai/prompts.py`](../../atlas/ai/prompts.py)
- [`atlas/ai/enricher.py`](../../atlas/ai/enricher.py)

Public exports now include:

- `AIConfig`
- `ModelInfo`
- `AIError`
- `AIConfigError`
- `AIConnectionError`
- `AITimeoutError`
- `AIGenerationError`
- `LocalLLMClient`
- `OllamaClient`
- `LlamaCppClient`
- `OpenAICompatibleClient`
- `build_client`
- `auto_detect_client`
- `SamplePreparer`
- `COLUMN_PROMPT_TEMPLATE`
- `TABLE_PROMPT_TEMPLATE`
- `ResponseParser`
- `SemanticEnricher`

The whole phase remains local-first. It supports local LLM runtimes and local
OpenAI-compatible gateways only. No remote-provider authentication or external
API integration is part of the delivered contract.

## Phase 8A client contract

`AIConfig` loads from:

- Python dicts through `AIConfig.from_dict()`
- TOML files through `AIConfig.from_file()`

Configuration fields:

- `provider`
- `model`
- `base_url`
- `temperature`
- `max_tokens`
- `timeout_seconds`

Validation rules:

- `temperature >= 0.0`
- `max_tokens >= 1`
- `timeout_seconds > 0.0`

Supported concrete clients:

- `OllamaClient`
- `LlamaCppClient`
- `OpenAICompatibleClient`

Provider endpoints:

- Ollama: `GET /api/version`, `POST /api/generate`
- llama.cpp HTTP server: `GET /health`, `POST /completion`
- local OpenAI-compatible servers: `GET /v1/models`, `POST /v1/chat/completions`

Detection rules:

- `build_client(config)` respects `config.provider`
- `build_client(config)` delegates to `auto_detect_client(config)` when
  `provider == "auto"`
- `auto_detect_client(config)` probes providers in the order:
  `OllamaClient`, `LlamaCppClient`, `OpenAICompatibleClient`

Error mapping:

- request timeout -> `AITimeoutError`
- unreachable endpoint -> `AIConnectionError`
- invalid JSON or unexpected response shape -> `AIGenerationError`

Operational limits:

- clients return raw text only; JSON extraction is intentionally delegated to
  `ResponseParser`
- no streaming support is implemented in Phase 8

## Phase 8B semantic-firewall contract

`SamplePreparer` is a pure in-memory transformer over already collected sample
rows.

Phase 8B exposes:

- `detect_pattern(value)`
- `prepare_column_context(column, sample_rows, privacy_mode)`
- `prepare_table_context(table, sample_rows, privacy_mode)`

Recognized pattern tags:

- `EMAIL`
- `UUID`
- `CPF_BR`
- `CNPJ_BR`
- `ISO_DATE`
- `CURRENCY_BR`

PII handling rules:

- `EMAIL`, `CPF_BR`, and `CNPJ_BR` are always replaced with `[PATTERN: ...]`
  in `PrivacyMode.normal`
- `stats_only` and `no_samples` always return `samples == "[]"`
- masked values already delivered as `***` are passed through without reverse
  inference

Budget rules:

- distinct values are deduplicated
- `max_distinct_values` defaults to `20`
- the sample-string budget is capped at roughly `800` characters
- overflow appends `# +N more`

Returned table-context keys:

- `table_name`
- `schema`
- `table_type`
- `row_count`
- `top_columns_summary`
- `fk_summary`
- `heuristic_classification`

Returned column-context keys:

- `column_name`
- `native_type`
- `nullable`
- `distinct`
- `null_rate`
- `pattern`
- `samples`

Operational limits:

- `SamplePreparer` does not fetch data by itself
- table-level context does not include per-row value dumps

## Phase 8C prompt and enrichment contract

Prompt templates are single-string constants:

- `TABLE_PROMPT_TEMPLATE`
- `COLUMN_PROMPT_TEMPLATE`

They are:

- JSON-first
- zero-shot
- free of markdown wrappers in the expected answer format

`ResponseParser.extract_json(raw_text)` accepts:

1. plain JSON objects
2. markdown fenced JSON blocks
3. JSON objects surrounded by arbitrary text, extracted by balanced-brace scan

`SemanticEnricher(client, sampler=None)` exposes:

- `enrich_table(table, sample_rows, privacy_mode)`
- `enrich_column(table, column, sample_rows, privacy_mode)`

Mutation contract:

- `enrich_table()` writes into:
  - `table.semantic_short`
  - `table.semantic_detailed`
  - `table.semantic_domain`
  - `table.semantic_role`
  - `table.semantic_confidence`
- `enrich_column()` writes into:
  - `column.semantic_short`
  - `column.semantic_detailed`
  - `column.semantic_role`
  - `column.semantic_confidence`

Retry contract:

- only `AITimeoutError` is retried
- maximum attempts: 3 total
- backoff schedule: `1.0s`, then `2.0s`
- `AIConnectionError` without timeout, `AIGenerationError`, and `KeyError`
  are not retried

Fallback rules:

- missing string fields become `None`
- invalid or missing `confidence` becomes `0.0`

Operational limits:

- Phase 8 enriches one table or one column at a time
- no cache, no parallel execution, and no schema-wide orchestration are part of
  this phase
