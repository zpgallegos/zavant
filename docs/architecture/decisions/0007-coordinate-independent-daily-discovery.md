# ADR 0007: Coordinate independent schedule and correction discovery daily

- Status: accepted
- Date: 2026-08-09

## Context

The local acquisition components can retrieve an explicit schedule range and can durably poll MLB's corrected-game feed, but neither operation alone expresses the intended once-daily production behavior. A daily run must discover newly final games, reconsider recently deferred schedule entries, retrieve corrections to games already in the portfolio, and recover safely when only one part fails.

The schedule endpoint has calendar boundaries rather than an `updatedSince` filter. Querying only the new calendar date can miss a recent postponed or deferred game whose state changes later. Querying the whole season daily catches that state but repeats unnecessary source work. Likewise, using the corrected-game feed for initial acquisition would bypass the explicit regular-season eligibility policy and could admit out-of-scope games.

## Decision

Maintain two independent discovery checkpoints:

- A correction timestamp advanced according to ADR 0006 after complete change-feed polling.
- A schedule through-date advanced only after its bounded acquisition manifest has no failed or pending games.

Incremental schedule discovery queries through the current UTC date. After bootstrap, its start date is a configurable rolling window ending on the previous successful through-date; the default window is seven days. The bounded acquisition policy re-evaluates every schedule entry, but an eligible game with an existing current raw revision is recorded as already landed without another live-feed request.

Completed correction manifests form a durable work queue. Their pending and failed games are retried, while succeeded and skipped games are terminal. A corrected game is re-downloaded only if its season/game key already has a current raw revision. Otherwise it is marked skipped because schedule discovery owns initial portfolio inclusion. Successful retrieval goes through the same contract and content-addressed raw store as initial acquisition, with `game_changes` recorded as its trigger.

One `run-daily` coordinator invocation executes and records these branches:

```text
correction discovery
    → process all durable correction work
    → rolling schedule discovery and initial acquisition
```

Every branch writes its own source manifest and state. A separate daily manifest links their results and reports aggregate success. Expected operational failure in one branch is recorded but does not prevent later branches from running. Consequently, successful checkpoint movement is not rolled back when another branch fails, and the next invocation retries only outstanding state.

The first daily invocation requires both an initial schedule date and an initial correction timestamp. If a partial bootstrap initializes only one branch, the next invocation supplies only the still-missing bootstrap value.

## Alternatives considered

- Use one shared daily watermark. This couples unrelated completeness conditions and either repeats successful work or risks skipping failed work.
- Query only dates after the schedule through-date. This does not reconsider recent deferred or postponed games.
- Query the current season every day. It is simple but produces unnecessarily large schedule snapshots and local manifest work.
- Re-download every game in the changes feed. This bypasses schedule eligibility and turns correction discovery into an accidental second ingestion policy.
- Stop the daily run after the first branch failure. This delays independent work and makes recovery slower without improving correctness.
- Deduplicate correction and schedule live-feed requests across a daily run. This requires cross-branch mutable coordination; content-addressed landing already makes rare overlap safe, and branch independence is more valuable at this stage.

## Consequences

- One command now completes local MLB API-to-raw acquisition for both new and corrected games.
- Recent schedule state is reconsidered with one bounded request while existing raw games avoid redundant live-feed calls.
- Correction failures remain explicitly retriable even after the discovery watermark advances.
- Every daily run explains all branch outcomes and links to their detailed manifests.
- The original seven-day correctness limitation is superseded by ADR 0019's
  durable deferred-game worklist. The lookback remains a schedule-discovery
  optimization.
- This completes local raw acquisition, not analytical publication, transformation, semantic modeling, or presentation.
