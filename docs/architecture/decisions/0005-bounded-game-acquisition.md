# ADR 0005: Drive bounded acquisition from durable schedule manifests

- Status: accepted
- Date: 2026-08-09

## Context

The contracts, local stores, and MLB client establish independent boundaries but do not yet form an operational workflow. The legacy downloader mixed schedule discovery, game eligibility, bucket scans, network requests, and writes in one function. It skipped any game already found in storage and therefore could not express durable expected work, individual failure, or safe resumption.

Schedule responses can change after they are requested. Resuming an interrupted run by requesting the schedule again could silently change the run's expected-game set.

## Decision

Implement a bounded acquisition service with this sequence:

```text
retrieve schedule
    → validate and land immutable schedule evidence
    → evaluate every discovered game with a named policy
    → retrieve and validate each eligible live feed
    → land its content-addressed raw revision
    → atomically record the per-game outcome
    → derive the run summary
```

The initial portfolio scope acquires games whose MLB `gameType` is `R` and `codedGameState` is `F`. Unfinished regular-season games are `deferred` so a later schedule run can reconsider them. Other game types are `skipped` with an explicit reason. The policy is isolated from HTTP and storage code and is independently tested.

Per-game network, validation, identity, and raw-storage failures are recorded as `failed`; they do not stop other games in the range. A run is `complete` when every game is succeeded, skipped, or deferred, and `failed` when one or more games failed.

A run is identified by both `run_id` and `requested_at`. Resumption with those values loads the exact stored schedule bytes rather than calling the schedule endpoint again. Succeeded, skipped, and deferred games are terminal within that immutable schedule snapshot. Pending and failed games are attempted again, and every outcome is appended to the game's attempt history.

## Alternatives considered

- Request the schedule again during resumption. This is simple but permits the expected-work set to change inside one run.
- Stop the range after the first game failure. This reduces partial state but delays unrelated games and provides poor behavior for large backfills.
- Infer completion by scanning raw game objects. This cannot distinguish expected, skipped, deferred, and failed games and repeats the legacy weakness.
- Embed eligibility in the orchestration loop. This obscures product scope and makes policy changes harder to test and review.
- Treat deferred games as failed. A scheduled or in-progress game is expected source state, not an operational failure.

## Consequences

- Every bounded run explains the outcome of every game returned by its schedule snapshot.
- Interrupted and partially failed local runs can resume without schedule drift or duplicate completed work.
- Raw-game idempotency makes recovery safe when a game landed before its manifest update failed.
- The current policy deliberately excludes postseason, exhibition, spring-training, and other non-regular-season games.
- Manifest updates are atomic on the local filesystem but not concurrency-controlled across multiple writers. The future S3 adapter must implement compare-and-set or equivalent single-writer semantics.
