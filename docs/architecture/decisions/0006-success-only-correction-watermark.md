# ADR 0006: Advance correction watermarks only after complete polling

- Status: accepted
- Date: 2026-08-09

## Context

An MLB live-game response can change after its first publication. The legacy design treated the presence of a game object as completion, so it had no durable mechanism for discovering and retrieving corrected games. MLB exposes a corrected-game feed keyed by `updatedSince`, but the resource has only a lower time boundary and uses offset pagination. A failure after some pages, an imprecise timestamp boundary, or overlapping scheduled runs could otherwise cause missed corrections.

Correction discovery and corrected-game processing also have different completion rules. Discovery can be complete even while its games remain pending for later live-feed retrieval. Combining both into one watermark would make failure recovery and run evidence harder to reason about.

## Decision

Maintain an independent corrected-game polling watermark with this sequence:

```text
capture run start T0
    → read prior logical watermark W
    → query from W minus a small safety overlap
    → validate and land every expected response page
    → finalize a deduplicated pending-game manifest
    → compare stored state and advance the watermark to T0
```

The initial poll requires an operator-supplied last-known-safe watermark. Later polls reject a new bootstrap value and use only durable state. The default overlap is five minutes. Repeated games are expected and harmless because manifests deduplicate game IDs and raw-game landing is content-addressed.

The item total from the first response defines the expected page count for that run. Every page is retained with exact bytes, request provenance, and checksum. A poll with no changed games still lands one empty response page. A configurable maximum-page guard prevents unexpected source volume from creating an unbounded run.

The manifest is marked complete only after its page sequence and pagination offsets validate. Watermark advancement additionally verifies that the referenced manifest is complete, belongs to the same run, and records matching before/after boundaries. If retrieval, validation, persistence, or finalization fails, the prior watermark remains unchanged and any partial manifest remains open for diagnosis.

The local adapter atomically replaces state files and performs compare-before-write validation. Scheduled local or production operation must maintain one active correction poll at a time; the future S3 adapter must use a conditional write or another storage-native compare-and-set mechanism.

## Alternatives considered

- Advance the watermark after each page. A later page failure could permanently skip unlanded results.
- Advance to the wall-clock time after polling. Corrections published while the request is running could fall behind the new checkpoint.
- Query exactly from the prior timestamp. Boundary precision, propagation delay, or clock behavior could omit a correction; a small overlap makes duplicates preferable to gaps.
- Infer the checkpoint from the newest returned game. The response does not expose the correction timestamp needed to establish a trustworthy high-water mark.
- Couple polling and live-feed retrieval into one transaction. This holds discovery progress hostage to individual games and obscures a durable pending-work boundary.

## Consequences

- A daily run can safely discover corrections accumulated since its previous successful poll.
- Failed polls retry from the old checkpoint, with overlap providing additional protection around the boundary.
- Completed manifests are the durable handoff to corrected-game retrieval, which remains a separate implementation slice.
- The watermark documents polling completeness, not completion of each pending game.
- Source offset pagination is not a true immutable snapshot. Capturing `T0` before requests plus the next run's overlap ensures changes arriving after `T0` are eligible again, while the page-count and page-content checks fail closed on obvious instability.
