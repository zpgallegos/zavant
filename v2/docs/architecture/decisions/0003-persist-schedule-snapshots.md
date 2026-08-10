# ADR 0003: Persist bounded schedule snapshots and discovery manifests

- Status: accepted
- Date: 2026-08-09

## Context

The MLB schedule is the authoritative discovery mechanism for the expected set of games in a requested date range. The legacy downloader used each response only in process memory, then inferred completeness by scanning game objects already present in storage. That makes it difficult to reproduce why a game was downloaded or skipped and provides no durable evidence when games are postponed, canceled, added, or rescheduled.

Schedule responses are mutable observations: requesting the same date range later can legitimately produce different game times or states. They are operational evidence rather than the final source for analytical game facts.

## Decision

Persist every bounded schedule request as an immutable run under:

```text
raw/mlb_stats_api/schedules/
  request_date=<UTC-date>/run_id=<uuid>/
    response.json
    metadata.json
    manifest.json
```

`response.json` retains the exact source bytes. `metadata.json` records the requested date range, sport ID, request time, source URI, response checksum, and source totals. A run ID cannot be reused with different source bytes or request provenance.

`manifest.json` contains one deduplicated entry per discovered game with the fields needed for later eligibility and live-feed retrieval. Each game begins with `processing_status: pending`. A later acquisition slice will own status transitions such as deferred, skipped, succeeded, and failed; the landing module does not embed a game-eligibility policy.

The schedule snapshot is discovery and audit evidence. Complete live-game feeds remain the primary source for analytical game data.

## Alternatives considered

- Do not persist schedules. This saves little storage but prevents request-level completeness checks and makes historical acquisition decisions difficult to explain.
- Maintain one mutable schedule file per date. This provides current state but erases evidence of rescheduling and status changes.
- Store only parsed game identifiers. This supports retrieval but loses the exact response and fields needed to investigate upstream changes.
- Treat the schedule as an analytical game dataset. This conflates discovery state with the richer complete-game source and would create competing definitions of game facts.

## Consequences

- Every acquisition run has a durable expected-game set and can report completeness without scanning stored games.
- Repeated observations preserve evidence of schedule changes.
- Schedule responses and manifests consume additional but comparatively small storage.
- The future API client must capture a request timestamp and assign a unique run ID before landing a response.
- Retention or content-deduplication policies can be added later if frequent identical schedule snapshots become noisy.
