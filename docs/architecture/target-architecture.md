# Target architecture

Zavant will be rebuilt as a local-first analytics product whose complete path—from source response to user-facing metric—is reproducible and observable.

## System boundaries

```text
MLB Stats API
    |
    v
Acquisition --------> immutable raw object store
    |                         |
    |                         v
    +-----------------> validated analytical datasets
                              |
                              v
                        dbt transformations
                              |
                              v
                       semantic definitions
                              |
                              v
                    API / application / notebooks
```

Acquisition begins with a bounded schedule snapshot. The exact response and its request provenance are retained, while a run manifest records the games discovered for eligibility and retrieval processing. This provides the expected-work set used to measure completeness. Each eligible complete-game response is validated before revision-aware landing, and each outcome is atomically recorded without preventing independent games from proceeding. Resumption reuses the stored schedule snapshot and retries only unfinished or failed work.

Acquisition also includes a correction loop. The poller captures a run-start checkpoint, queries MLB's corrected-game feed from the prior durable watermark with a small safety overlap, and produces immutable response pages plus a deduplicated run manifest. The watermark advances to the captured checkpoint only after every expected page is durable and the manifest is marked complete. Each pending changed game is then retrieved from its complete live-feed link and landed as a new content-addressed revision when its meaningful JSON content differs. The prior revision remains available, while a small pointer identifies the revision downstream processing should currently use.

Daily operation coordinates four independently recorded branches: correction
discovery, correction processing, durable deferred-game processing, and schedule
discovery. The correction processor retries pending and failed work from every
completed poll but re-downloads only games already present in raw storage;
schedules own initial portfolio inclusion. Schedule discovery maintains a
separate successful through-date and a rolling lookback for cheap discovery.
Non-final regular-season games also enter a durable worklist and are checked
daily until final or cancelled, so completeness does not depend on the lookback
window. One branch failure does not suppress the others or roll back successful
checkpoints.

The same logical boundaries work locally and in the cloud. Local development
uses atomic filesystem publication. Production uses the same logical keys and
persistence state machines over S3, with ETag preconditions protecting mutable
objects. A Lambda composes the acquisition run; explicit Python mappings project
every source revision into revision-aware analytical tables. Local Parquet
runs exercise those contracts without AWS. A Glue Spark job independently
reconciles every current pointer against its completed-revision registry and
merges missing revisions into Iceberg v2. EventBridge Scheduler starts one
Standard Step Functions execution that invokes acquisition and waits for Glue
projection, rather than invoking either component on an independent schedule.
Glue reconciliation still runs when acquisition partially fails, after which
the workflow reports the retained acquisition error.

## Layers and responsibilities

| Layer | Owns | Does not own |
|---|---|---|
| Acquisition | API interaction, retry policy, provenance, raw persistence | Baseball metrics |
| Contracts | Source validation, compatibility checks, dataset keys and types | Dashboard formatting |
| Analytical storage | Columnar datasets, partitions, safe incremental publication | Business definitions |
| Transformation | Tested dimensions, facts, and intermediate models | HTTP/API concerns |
| Semantics | Metrics, entities, dimensions, time grains, access contracts | Raw API parsing |
| Presentation | Exploration and product experiences | Reimplementing metrics |
| Infrastructure | Repeatable local and cloud deployment, secrets, observability | Hidden manual setup |

## Initial technology decisions

- Python is the acquisition and platform language.
- HTTP retrieval is storage-neutral, uses explicit timeouts, and retries only bounded transient failures.
- dbt remains the SQL transformation framework unless migration evidence shows it is a poor fit.
- Raw responses are immutable and retain their original bytes.
- Complete games are content-addressed by canonical JSON and retain all observed revisions.
- Bounded schedule responses are retained as immutable discovery snapshots with per-run manifests.
- Corrected-game polls retain their page responses and deduplicate work in a run manifest.
- Correction polling uses an independent success-only watermark; overlapped queries favor harmless duplicate work over missed updates.
- Schedule discovery uses an independent through-date and rolling lookback;
  durable deferred-game state guarantees later reconsideration, and current raw
  pointers prevent redundant game downloads.
- Daily coordinator manifests link all acquisition evidence without coupling branch checkpoint advancement.
- Object paths are deterministic and include named partitions.
- Every landed object has provenance metadata and a checksum.
- Filesystem-backed execution is first-class, not a mock of the cloud implementation.
- Production acquisition runs as one Lambda over a conditionally written S3
  prefix and is invoked by the daily state machine rather than directly by its
  schedule.
- Analytical datasets use explicit grain-specific contracts; pitches and
  batted-ball metrics are extensions of a skinny play-event spine.
- Every analytical row retains its source revision; the projection contract is
  lineage for the single active analytical generation.
- Local analytical runs publish atomic Parquet snapshots; production tables use
  Iceberg format version 2 in S3 and the Glue Data Catalog for Athena access.
- Production projection scans every immutable raw revision rather than relying
  on the preceding acquisition manifest. The terminal `games` merge makes
  retries repair partial multi-table publication. Glue-owned `current_*`
  Athena views resolve `current_game_revisions` and give dbt business-grained
  current state without revision keys.
- The daily workflow stack owns EventBridge Scheduler and the Step Functions
  Standard workflow that sequences acquisition and one batch Glue projection;
  later dbt execution can become a subsequent state.
- Cloud infrastructure is defined as code and separated by workload ownership.

The semantic-layer implementation and presentation framework remain deliberate
follow-up decisions. They should be selected with thin vertical slices and
recorded as ADRs rather than inherited accidentally from the legacy stack.

## Environments

- **Test:** temporary isolated storage and recorded fixtures; no network or cloud credentials.
- **Local:** `.local/` data, developer-selected dates or games, and the same contracts used in production.
- **Production:** S3 raw/state storage, Step Functions-orchestrated Lambda and
  Glue execution, a catalog/query engine, least-privilege IAM, and centralized
  telemetry.

Configuration enters at process boundaries through environment variables or explicit command options. Domain and transformation code must not contain account IDs, bucket names, seasons, or credentials.

## Quality gates

Each migrated vertical slice must provide:

1. A source or recorded fixture.
2. A versioned boundary contract.
3. Idempotency behavior.
4. Unit and integration tests.
5. Data-quality assertions at its output.
6. Observable run metadata and actionable failures.
7. Documentation of any material design decision.
