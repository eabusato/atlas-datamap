# Privacy Modes

Atlas exposes four privacy modes:

- `normal`
- `masked`
- `stats_only`
- `no_samples`

## Operational contract

- `normal`: sample rows may contain raw values.
- `masked`: sample rows are allowed, but columns whose names match the sensitive
  patterns from [`atlas/config.py`](../atlas/config.py)
  are replaced with `***`.
- `stats_only`: row sampling is blocked. Aggregate statistics remain allowed.
- `no_samples`: no live row access is allowed.

This started as a connector-level rule in the earliest database support and is
now part of the shared Atlas connector contract.

## Phase 1 notes

- The PostgreSQL connector enforces the same privacy modes before sampling.
- `masked` mode applies redaction after fetching rows and before returning them
  to callers.
- Aggregate metadata such as row counts, relation sizes, and `pg_stats`
  estimates remain available even when sampling is blocked.

## Current limits

- The privacy contract is enforced in the base connector helpers and inherited
  by the current connectors.
- Sensitivity is name-based only.
- Privacy is explicit per configuration and per sampling call; there is no
  process-wide mutable mode.

That means Atlas already provides useful privacy controls, but `masked` should
be read as a practical redaction mode, not as a full data-loss-prevention
system. Columns whose names do not look sensitive may still contain personal or
business-sensitive values.

Examples of what this means in practice:

- names, labels, free-text notes, and business identifiers may remain visible
  in `masked` mode when their column names do not match the sensitive-name list
- some structured values may still be represented in prompt context when the
  active workflow allows samples
- `stats_only` and `no_samples` are the stronger modes when the operator wants
  to avoid sample-derived prompt context entirely

## AI and prompt boundary

Atlas supports local-first semantic workflows, but there is an important
operational distinction between onboarding and manual AI configuration.

- `atlas onboard` restricts AI configuration to local hosts such as
  `localhost`, `127.0.0.1`, and `::1`
- manual use of `atlas enrich` and `atlas ask` with a custom AI config can
  point to another endpoint if the operator chooses to do so
- when AI enrichment or QA is enabled, Atlas sends structural metadata and the
  configured privacy-aware prompt context to that AI endpoint

In other words: Atlas does not silently exfiltrate metadata on its own, but the
effective trust boundary includes the AI endpoint explicitly configured by the
user for that run.

## Local artifacts and operator responsibility

Atlas is designed to write its outputs locally, but many of those outputs are
intentionally rich metadata artifacts.

- `.sigil`, `_meta.json`, `.atlas`, JSON, CSV, Markdown, and HTML outputs may
  contain schema names, table names, column names, comments, semantic
  descriptions, inferred roles, row-count estimates, and other structural
  metadata
- onboarding keeps secrets in local env handling and stores env-var references
  in the manifest rather than resolved secret values
- local artifacts should still be treated as sensitive project material when
  the database structure itself is confidential

Recommended operational stance:

- use `atlas onboard` for the safest default setup
- prefer `stats_only` or `no_samples` for regulated or unknown datasets
- use a local AI runtime for semantic features when metadata must stay on the
  same machine
- review exports and generated artifacts before sharing them outside the team

## Onboarding and local-only guarantees

The `atlas onboard` flow is designed to keep operational data local:

- secrets can be stored in a local `.env` file inside the workspace chosen by
  the user
- the onboarding manifest stores only env-var references, not the resolved
  secret values
- Atlas does not upload those secrets, snapshots, or schema metadata to third
  parties on its own
- database traffic goes only to the configured database endpoint
- onboarding restricts AI configuration to local endpoints such as
  `localhost`, `127.0.0.1`, and `::1`

That means the onboarding flow will not silently forward credentials or schema
content outside the user's environment. If a user chooses a remote database,
traffic still goes only to that explicitly configured database service.
