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

## Phase 1 notes

- The PostgreSQL connector enforces the same privacy modes before sampling.
- `masked` mode applies redaction after fetching rows and before returning them
  to callers.
- Aggregate metadata such as row counts, relation sizes, and `pg_stats`
  estimates remain available even when sampling is blocked.

## Current limits

- The privacy contract is enforced in the base connector helpers and in the
  current PostgreSQL connector.
- Sensitivity is name-based only in this phase.
- Privacy is explicit per configuration and per sampling call; there is no
  process-wide mutable mode.

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
