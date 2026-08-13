# ADR 0016: Reconcile current raw revisions into Iceberg

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
idempotent publication, and a durable definition of which revision/contract
combinations are complete.

## Decision

Run production projection as an AWS Glue 5.0 Spark job independently of the
acquisition work list. Each invocation enumerates every raw-game `current.json`
pointer and anti-joins `(game_pk, source_revision_id,
projection_contract_version)` against an Iceberg `projection_revisions`
registry. Only absent combinations are projected.

The driver performs reconciliation and distributes pending revision descriptors
to Spark workers. Workers load and validate the selected raw object and reuse
the pure Python per-game projector. The resulting projections are persisted in
Spark and merged into each contract table by its natural key.

The job creates format-version-2 Iceberg tables in an explicit Glue Data Catalog
database and S3 warehouse. It writes `projection_revisions` only after every
analytical table merge succeeds. A separate `current_game_revisions` table
records the current raw revision for each game and contract version, giving
Athena and dbt a queryable current-state join.

Iceberg does not provide one transaction across all 25 tables. If a run fails
after a subset of merges, its completion registry remains absent. A retry
selects the same revision and repairs every table through deterministic merges.
The Glue job allows only one concurrent run.

The Glue resources live in a separate `zavant-analytics-{environment}`
CloudFormation stack. The stack receives the retained acquisition bucket as an
input and owns only the analytical database, projection role, and Glue job.
Step Functions orchestration is defined separately in ADR 0017 after the job's
independent deployment and smoke test.

## Consequences

Projection correctness does not depend on how a game arrived or whether a prior
workflow completed. A contract-version change naturally selects every current
game for reprojection. Historical raw revisions remain durable but are not
projected unless they are selected by a current pointer.

Every job performs a full listing and read of current pointers. This is simple,
observable, and inexpensive at MLB scale, but acquisition manifests may later
serve as a fast-path candidate list if necessary. A periodic full reconciliation
must remain the correctness backstop.

The registry is the publication gate rather than evidence of a cross-table
transaction. Downstream current models must join through
`current_game_revisions` and should run only after a successful Glue invocation.

## Alternatives considered

- Project only games named in the daily acquisition manifest. Rejected because
  failed or out-of-band projection can leave permanent gaps.
- Trigger one Lambda per raw object. Rejected because backfills create small
  commits, concurrency, and Lambda duration pressure.
- Write Iceberg directly from a Python-shell job. Rejected for the first
  production implementation because Glue Spark has native Iceberg merge and
  Glue Catalog support and scales the existing per-game mapping across workers.
