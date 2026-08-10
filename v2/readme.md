# Zavant v2

This directory is the ground-up modernization of Zavant. It is being built through tested vertical slices while [`../v1`](../v1/readme.md) remains available as the behavioral and historical reference.

The current acquisition foundation validates and locally lands three recorded MLB Stats API response types:

- Complete live-game feeds, stored as immutable content-addressed revisions with a pointer to the current revision.
- Bounded schedule snapshots, stored by request run with a manifest of games discovered for later eligibility and retrieval processing.
- Pages returned by the corrected-game change feed, stored by poll run with a manifest of games that need to be retrieved again.

It has no third-party runtime dependencies and bootstrap works offline. A typed MLB API client supports schedule, corrected-game, and complete live-game requests with explicit timeouts and bounded retries. The complete local acquisition workflow now combines incremental schedule discovery, corrected-game polling and processing, revision-aware raw landing, independent success watermarks, and a durable daily coordinator manifest.

## Local development

From this directory:

```shell
make bootstrap PYTHON=/path/to/python3.12
make check
make ingest-sample
make ingest-schedule-sample
make ingest-changes-sample
```

If `python3` already points to Python 3.9 or newer, `make bootstrap` is sufficient. Local outputs are written under `.local/` and ignored by Git.

The sample commands create these layouts:

```text
.local/lake/
├── raw/mlb_stats_api/
│   ├── games/season=2024/game_pk=744863/
│   │   ├── revision=<canonical-sha256>/
│   │   │   ├── game.json
│   │   │   └── metadata.json
│   │   └── current.json
│   ├── schedules/request_date=2026-08-09/
│   │   └── run_id=00000000-0000-0000-0000-000000000002/
│   │       ├── response.json
│   │       ├── metadata.json
│   │       └── manifest.json
│   └── game_changes/poll_date=2026-08-09/
│       └── run_id=00000000-0000-0000-0000-000000000001/
│           ├── page=0000/
│           │   ├── response.json
│           │   └── metadata.json
│           └── manifest.json
├── state/mlb_stats_api/
│   ├── game_changes/watermark.json
│   └── schedules/watermark.json
└── runs/daily/run_date=2026-08-09/
    └── run_id=<uuid>/manifest.json
```

`game.json` and both kinds of `response.json` retain the exact source bytes. A game revision ID is a SHA-256 digest of canonical JSON, so whitespace and object-key order do not create false revisions. `current.json` advances when a new revision is first landed and is not rolled backward if an older known revision is replayed. Schedule and change manifests deduplicate game IDs and initially mark each one `pending` for their respective downstream workflows.

## MLB API client

[`MlbStatsApiClient`](src/zavant/clients/mlb_stats_api.py) provides typed methods for the three public resources currently in scope:

- `get_schedule(start_date, end_date, sport_id=1)`
- `get_game_changes(updated_since, sport_id=1, limit=1000, offset=0)`
- `get_live_game(game_pk)`

The client returns exact response bytes and HTTP provenance; the contract classes validate the resource-specific JSON afterward. Its standard-library transport is replaceable in tests, each request has an explicit timeout, and retry behavior is bounded and configurable. The file-landing CLI commands remain offline development tools rather than making implicit network requests.

## Bounded game acquisition

Download a bounded range from the live API with:

```shell
PYTHONPATH=src .venv/bin/python -m zavant acquire-games \
  --start-date 2024-03-20 \
  --end-date 2024-03-20
```

The workflow:

1. Captures and lands the exact bounded schedule response.
2. Classifies every discovered game with the explicit final-regular-season policy.
3. Downloads and validates every eligible complete live-game response.
4. Lands each response through the revision-aware raw-game store.
5. Atomically records per-game attempts and the final run summary in the schedule manifest.

Final regular-season games are acquired, unfinished regular-season games are `deferred`, and other game types are `skipped`. Individual download or validation failures are recorded as `failed` without preventing independent games from running. The command exits with status 1 when any game failed.

The command prints its generated `run_id` and `requested_at`. Supply both values with the same date range to resume that exact run:

```shell
PYTHONPATH=src .venv/bin/python -m zavant acquire-games \
  --start-date 2024-03-20 \
  --end-date 2024-03-20 \
  --run-id <previous-run-id> \
  --requested-at <previous-requested-at>
```

A resume reads the persisted schedule instead of requesting a potentially changed snapshot. It retries pending and failed games while leaving succeeded, skipped, and deferred entries unchanged.

## Corrected-game polling

Bootstrap the correction feed once with an explicit last-known-safe timestamp:

```shell
PYTHONPATH=src .venv/bin/python -m zavant poll-game-changes \
  --initial-watermark 2024-03-20T00:00:00Z
```

Every later run reads its checkpoint from `.local/lake/state/mlb_stats_api/game_changes/watermark.json`, so `--initial-watermark` must be omitted:

```shell
PYTHONPATH=src .venv/bin/python -m zavant poll-game-changes
```

The poller captures its new checkpoint before making a request, subtracts a five-minute safety overlap from the prior checkpoint, follows the source's offset pagination, and lands every response page. It then validates and marks the run manifest `complete` before atomically replacing the watermark document. Any request, validation, landing, or finalization failure leaves the old watermark unchanged; already landed pages remain as evidence in an `open` manifest.

The overlap intentionally allows a game to appear again in later polls. Manifests deduplicate game IDs, and raw-game content addressing makes reprocessing idempotent. The daily workflow processes every completed correction manifest containing pending or failed games. Only games with an existing raw revision are re-downloaded; schedule discovery remains the sole owner of initial portfolio inclusion. Per-game failures remain retriable in the source correction manifest.

The default page size is 1,000, the safety guard is 100 pages, and the overlap is 300 seconds. These can be overridden with `--limit`, `--max-pages`, and `--overlap-seconds`.

## Daily local workflow

Bootstrap both independent discovery checkpoints once:

```shell
PYTHONPATH=src .venv/bin/python -m zavant run-daily \
  --initial-schedule-date 2024-03-20 \
  --initial-correction-watermark 2024-03-20T00:00:00Z
```

Then run the same workflow daily without bootstrap arguments:

```shell
PYTHONPATH=src .venv/bin/python -m zavant run-daily
```

Each invocation records three independent branches in `runs/daily/.../manifest.json`:

1. Poll and durably checkpoint corrected-game discovery.
2. Retry all pending or failed corrected games that already exist locally.
3. Discover the schedule through the current UTC date and acquire newly final regular-season games.

The schedule branch keeps its own successful through-date and, by default, re-queries a rolling seven-day window. This reconsideration catches recent deferred, postponed, or changed schedule entries. Final games that already have a current raw revision are resolved from local state without another live-feed request. Use `--schedule-lookback-days` to tune the window and `--through-date` for a deterministic local run.

A failure in one branch does not prevent the others from running. The command exits with status 1 if any branch fails, while each successful discovery watermark advances independently. An immediate rerun therefore retries only the state that remains outstanding. If only one checkpoint was initialized during a partially successful bootstrap, supply only the missing branch's bootstrap argument on the next run.

This completes the local API-to-raw acquisition workflow. Analytical dataset publication, dbt transformation, semantics, and presentation remain later layers of the project.

See the [target architecture](docs/architecture/target-architecture.md), [decision register](docs/architecture/README.md), and [migration roadmap](docs/migration-roadmap.md) for the intended path from acquisition through semantics and presentation.
