# ADR 0019: Persist deferred-game work independently of schedule lookback

- Status: Accepted
- Date: 2026-08-11

## Context

ADR 0007 used a rolling schedule lookback to reconsider unfinished games. That
is efficient for ordinary finalization but makes completeness depend on a game
becoming final before it leaves the window. A long suspension or postponement
could therefore remain absent from raw storage indefinitely.

The corrected-game feed is not an initial-ingestion boundary: correction
processing intentionally skips games that schedule discovery has never landed.
It cannot safely repair this gap without bypassing the regular-season
eligibility policy.

## Decision

When schedule acquisition classifies a regular-season game as deferred, upsert
its identity and live-feed link into one conditionally written state document at
`state/mlb_stats_api/schedules/deferred_games.json`. Keep the immutable schedule
manifest as the audit record; the state document contains only unresolved work.

Add a daily deferred-game branch before schedule discovery. For every retained
game, retrieve and validate the current live feed:

- retain non-final games for the next daily run;
- land final games through the existing content-addressed raw store and remove
  them from the worklist;
- remove cancelled games as terminally ineligible; and
- retain games whose retrieval or validation failed while marking the branch
  failed and allowing later daily branches to continue.

Schedule discovery still uses its seven-day rolling window. The window now
optimizes discovery and captures nearby schedule changes; it is no longer the
guarantee that deferred games are reconsidered.

## Consequences

The steady-state cost is one live-feed request per unresolved game per daily
run. MLB-scale deferred counts should normally be very small, and the cost is
bounded by actual unresolved work rather than a widening calendar scan.

The daily manifest contract advances to v2 and records four branches. Local and
S3 execution reuse the same deferred-game state machine. Backfills retain their
existing season-scoped deferred reconciliation because their completion and
retry lifecycle differs from daily acquisition.

## Alternatives considered

- Widen the schedule query to the oldest unresolved official date. Rejected
  because one old suspension could cause an increasingly large daily response
  and repeated per-game storage checks.
- Let the correction feed introduce unseen games. Rejected because that bypasses
  the schedule-owned portfolio eligibility decision.
- Persist only official dates and query each date again. Rejected because the
  already-known live-feed identity is a smaller and more direct check.
