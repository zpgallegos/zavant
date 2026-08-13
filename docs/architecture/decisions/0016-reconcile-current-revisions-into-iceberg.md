# ADR 0016: Reconcile immutable raw revisions into Iceberg

- Status: Accepted
- Date: 2026-08-10

## Context

The acquisition workflow can land data through daily discovery, corrections,
historical backfills, and manual recovery. Using only the immediately preceding
daily manifest as the analytical work list would strand data whenever
acquisition succeeded but projection did not. An S3 object-created trigger
would also create one projection invocation per game and concurrent Iceberg
commits during backfills.

The analytical projector already maps one validated raw revision into 25
revision-aware table grains. Production needs a scalable execution boundary,
idempotent publication, and a durable definition of which source revisions are
complete.

## Decision

Run production projection as an AWS Glue 5.0 Spark job independently of the
acquisition work list. Each invocation enumerates every immutable raw-game
revision and anti-joins `(game_pk, source_revision_id)` against the `games`
Iceberg table. Only absent revisions are projected. This also allows an empty
analytical generation to rebuild superseded revision history from raw S3 data.

The driver performs reconciliation and distributes pending revision descriptors
to Spark workers. Workers load and validate the selected raw object and reuse
the pure Python per-game projector. The resulting projections are persisted in
Spark and merged into each contract table by its natural key.

The job creates format-version-2 Iceberg tables in an explicit Glue Data Catalog
database and S3 warehouse. It merges `games` after every other analytical table,
so its one-row-per-revision grain is the completion marker. A separate
`current_game_revisions` table records the current raw revision for each game,
giving Athena and dbt a queryable current-state join.

Iceberg does not provide one transaction across all 25 tables. If a run fails
before the terminal `games` merge, the revision remains incomplete. A retry
selects it again and repairs every table through deterministic merges. The Glue
job allows only one concurrent run.

The Glue resources live in a separate `zavant-analytics-{environment}`
CloudFormation stack. The stack receives the retained acquisition bucket as an
input and owns only the analytical database, projection role, and Glue job.
Step Functions orchestration is defined separately in ADR 0017 after the job's
independent deployment and smoke test.

## Consequences

Projection correctness does not depend on how a game arrived or whether a prior
workflow completed. Historical raw revisions are projected and remain
queryable after the current pointer advances.

Each physical analytical-table generation supports exactly one projection
contract. A new contract release requires rebuilding all analytical tables; the
job rejects existing `games` rows written by another contract version.

Every job performs a full listing of immutable revisions and current pointers.
This is simple and observable at MLB scale; acquisition manifests may later
serve as a fast-path candidate list if necessary. A periodic full reconciliation
must remain the correctness backstop.

The terminal `games` merge is the publication gate rather than evidence of a
cross-table transaction. Downstream current models must join through
`current_game_revisions` and should run only after a successful Glue invocation.

## Alternatives considered

- Project only games named in the daily acquisition manifest. Rejected because
  failed or out-of-band projection can leave permanent gaps.
- Trigger one Lambda per raw object. Rejected because backfills create small
  commits, concurrency, and Lambda duration pressure.
- Write Iceberg directly from a Python-shell job. Rejected for the first
  production implementation because Glue Spark has native Iceberg merge and
  Glue Catalog support and scales the existing per-game mapping across workers.
