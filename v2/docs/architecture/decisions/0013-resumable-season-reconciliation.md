# ADR 0013: Reconcile historical seasons outside the daily Lambda

- Status: Accepted
- Date: 2026-08-09

## Context

The daily workflow efficiently discovers new final games and corrections, but
it is intentionally bounded for one Lambda invocation. A historical import can
contain thousands of games, must survive partial failures, and needs an explicit
answer for games already present in raw storage. The public correction feed is
useful but does not provide an absolute signal for every metric-level revision.

## Decision

Provide a storage-neutral `backfill-seasons` CLI workflow rather than extending
the scheduled Lambda. It accepts multiple seasons and supports three modes:

- `missing` acquires only absent eligible games.
- `reconcile` acquires absent games and rechecks existing games named by the
  public correction feed since an independent per-season checkpoint.
- `verify` rechecks every eligible game and relies on raw content addressing to
  distinguish changed from identical responses.

The coordinator uses one deterministic schedule child run per calendar month.
A parent manifest records season outcomes, and a caller can resume with the
same run ID and start time. Completed correction discovery is also reused on
resume. Checkpoints advance independently by season only after successful
non-dry `reconcile` or `verify` work. Backfill correction pages use a separate
raw namespace and never enter the daily correction processing queue.

The command composes the same API client, eligibility policy, schedule store,
and raw-game store over either local files or S3.

## Consequences

Routine backfills avoid downloading every existing game, while an operator can
choose a stronger and more expensive full verification. Monthly units keep
schedule requests bounded and provide durable resumption. The season checkpoint
does not claim that the public correction feed covers metric or Statcast changes;
periodic `verify` runs remain necessary when that stronger guarantee matters.
