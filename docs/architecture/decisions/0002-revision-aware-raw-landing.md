# ADR 0002: Preserve game revisions and corrected-game poll evidence

- Status: accepted
- Date: 2026-08-09

## Context

MLB can correct a game's non-Statcast data after its complete live-feed response has already been published and landed. The legacy layout used one object key per game, so a later download either overwrote history or could not express that the source had changed. Scanning stored game objects also does not reveal which games MLB says should be retrieved again.

MLB exposes a corrected-game feed that accepts an `updatedSince` timestamp and returns changed games with links to their complete live feeds. A poll can have multiple response pages, and the same game can appear more than once.

## Decision

Store each meaningfully distinct complete-game response as an immutable revision under:

```text
raw/mlb_stats_api/games/
  season=<year>/game_pk=<id>/revision=<canonical-sha256>/
    game.json
    metadata.json
```

The revision identifier is the SHA-256 digest of canonicalized JSON. The exact downloaded bytes are retained in `game.json`, while metadata records both the byte-level and canonical digests. This makes replays with different whitespace or object-key order idempotent without discarding source fidelity.

Maintain `current.json` beside the revision directories as the mutable pointer to the latest newly observed revision. Replaying a known older revision does not move the pointer backward. Later orchestration must serialize updates for a game or provide equivalent compare-and-set behavior in the cloud adapter.

Store every corrected-game response page under a poll run:

```text
raw/mlb_stats_api/game_changes/
  poll_date=<UTC-date>/run_id=<uuid>/
    page=<zero-padded-number>/
      response.json
      metadata.json
    manifest.json
```

The manifest records the bounded poll window, page evidence, and one deduplicated entry per changed game with an initial processing status of `pending`. The API client, processing transitions, and durable watermark advancement are separate acquisition work. A watermark must advance only after every page has landed and all required work is durably represented.

## Alternatives considered

- Overwrite one object per game. This loses source history and makes corrections difficult to audit or reproduce.
- Version games by download time. This preserves every request but creates duplicate revisions when content is unchanged and makes comparison less direct.
- Use MLB's feed timestamp as the revision identifier. It is useful provenance, but a content digest independently verifies identity and still works when the field is missing.
- Store only the deduplicated changed-game list. This loses the exact API evidence and page-level request context needed for debugging and replay.

## Consequences

- Corrections are auditable and downstream models can explicitly select the current revision.
- Repeated downloads of semantically identical JSON do not create duplicate revisions.
- Storage grows with distinct source revisions and retained poll evidence.
- The mutable current pointer and future watermark require concurrency-safe publication in the S3 adapter.
- Canonical hashing treats JSON array order and numeric representation as meaningful, while ignoring object-key order and insignificant whitespace.
