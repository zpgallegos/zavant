# Zavant v2

This directory is the ground-up modernization of Zavant. It is being built through tested vertical slices while [`../v1`](../v1/readme.md) remains available as the behavioral and historical reference.

The current acquisition foundation validates and locally lands two recorded MLB Stats API response types:

- Complete live-game feeds, stored as immutable content-addressed revisions with a pointer to the current revision.
- Pages returned by the corrected-game change feed, stored by poll run with a manifest of games that need to be retrieved again.

It has no third-party runtime dependencies and bootstrap works offline. The network client and automatic change-to-game retrieval workflow are intentionally left for the next acquisition slice.

## Local development

From this directory:

```shell
make bootstrap PYTHON=/path/to/python3.12
make check
make ingest-sample
make ingest-changes-sample
```

If `python3` already points to Python 3.9 or newer, `make bootstrap` is sufficient. Local outputs are written under `.local/` and ignored by Git.

The sample commands create these layouts:

```text
.local/lake/raw/mlb_stats_api/
├── games/season=2024/game_pk=744863/
│   ├── revision=<canonical-sha256>/
│   │   ├── game.json
│   │   └── metadata.json
│   └── current.json
└── game_changes/poll_date=2026-08-09/
    └── run_id=00000000-0000-0000-0000-000000000001/
        ├── page=0000/
        │   ├── response.json
        │   └── metadata.json
        └── manifest.json
```

`game.json` and `response.json` retain the exact source bytes. A game revision ID is a SHA-256 digest of canonical JSON, so whitespace and object-key order do not create false revisions. `current.json` advances when a new revision is first landed and is not rolled backward if an older known revision is replayed. The change manifest deduplicates game IDs across pages and initially marks each one `pending`.

See the [target architecture](docs/architecture/target-architecture.md), [decision register](docs/architecture/README.md), and [migration roadmap](docs/migration-roadmap.md) for the intended path from acquisition through semantics and presentation.
