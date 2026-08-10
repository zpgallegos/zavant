# ADR 0008: Separate acquisition from storage backends

- Status: accepted
- Date: 2026-08-09

## Context

The complete local acquisition workflow established the required persistence behavior, but acquisition services were annotated with concrete `Local*Store` classes and exchanged filesystem `Path` values. Adding S3 directly to those classes would either spread bucket awareness through acquisition or create a second cloud-specific workflow. Both outcomes would make local/cloud parity difficult to prove.

The storage boundary includes more than byte writes. It owns immutable source evidence, content-addressed revisions, current pointers, resumable processing manifests, processable-work discovery, success-only watermarks, lineage validation, and conflict detection for mutable state.

## Decision

Acquisition depends on typed, domain-specific storage protocols rather than filesystem or S3 APIs. The protocols cover raw games, schedule runs, corrected-game polls, their independent watermarks, and daily coordinator manifests.

Persisted artifacts cross the boundary as an `ArtifactReference` containing:

- A canonical, relative POSIX key such as `raw/mlb_stats_api/games/season=2026/game_pk=1/current.json`.
- A backend-specific URI used only for logs and operator output, such as a local path or `s3://` URI.

The key is the portable identity stored in manifests and watermarks. Acquisition may pass an artifact reference back to the adapter that created it, but it may not open the URI or perform backend-specific path operations.

Adapters share these behavioral requirements:

- Exact source bytes and revision objects are immutable.
- Repeating an identical write is idempotent; conflicting content fails explicitly.
- Mutable manifests, pointers, and watermarks are published with conflict detection appropriate to the backend.
- A discovery watermark advances only after its supporting manifest is complete and durable.
- Watermarks retain the supporting run ID and manifest key.
- Logical object keys and versioned JSON contracts remain the same across backends.
- Adapter behavior is exercised through a shared protocol/conformance test surface. Offline tests use isolated local storage; cloud integration tests remain a separate gate.

Shared result and state models, storage conflicts, and protocols live outside the local adapter modules. Local adapters resolve artifact keys to filesystem paths internally and retain atomic temporary-file replacement. S3 adapters will resolve the same keys beneath a configured bucket and prefix and will use conditional or version-aware writes for mutable state.

## Alternatives considered

- Add S3 calls to the local stores. This obscures backend guarantees and makes local behavior conditional on configuration.
- Create a separate S3 acquisition workflow. This duplicates correction, eligibility, retry, and watermark logic and makes behavioral drift likely.
- Expose `Path | S3Uri` unions throughout acquisition. This leaks backend branching into domain services and provides weak type guarantees.
- Define only a generic byte-object interface. The important contract includes manifest state transitions, work discovery, revision lookup, and watermark compare-and-advance behavior; a byte API alone would not express those invariants.

## Consequences

- The existing API client, contracts, eligibility policy, acquisition services, and daily coordinator are reusable with S3.
- Local stores remain first-class production-shaped adapters rather than test doubles.
- Acquisition results now expose artifact references instead of directly readable filesystem paths.
- Local tests that inspect persisted bytes resolve the reference URI explicitly; acquisition code cannot do so.
- The future S3 implementation must satisfy the same domain protocols and preserve the established logical key layout.
- S3 concurrency, bucket versioning, lifecycle, IAM, and infrastructure choices remain a separate implementation ADR informed by the adapter contract.
