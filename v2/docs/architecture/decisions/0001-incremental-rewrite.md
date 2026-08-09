# ADR 0001: Replace the system through vertical slices

- Status: accepted
- Date: 2026-08-08

## Context

The legacy project under `v1/` contains valuable working logic and historical data, but its runtime configuration and AWS infrastructure are only partially represented in source control. Replacing every component at once would make it difficult to distinguish regressions from intentional changes.

## Decision

Keep `v1/landing/`, `v1/dbt/`, and `v1/app/` available as behavioral references. Build new capabilities in `v2` and replace one end-to-end slice at a time. A slice is complete only when it can be demonstrated locally, tested, and traced from its input to its consumer.

The first slices establish raw-game and corrected-game-feed persistence using checked-in MLB response fixtures. They establish package structure, configuration, contracts, deterministic storage, provenance, idempotency, correction history, and testing without depending on AWS.

## Consequences

- The repository temporarily contains legacy and replacement implementations.
- Migration progress can be reviewed as small, evidence-backed changes.
- Existing production resources are not modified merely to establish the new environment.
- Legacy code is deleted only after its replacement has parity and a documented cutover.
