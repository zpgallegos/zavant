# Migration roadmap

This is a sequence of independently demonstrable vertical slices, not a commitment to a big-bang rewrite.

## 0. Foundation — established

- Python package and command-line boundary
- Environment-based configuration
- Versioned raw-game, schedule, and corrected-game change-feed contracts
- Revision-aware local game storage with checksums, provenance, immutable history, and a current pointer
- Immutable local schedule snapshots and per-request discovery manifests
- Immutable local change-feed pages and per-poll manifests with deduplicated pending games
- Typed MLB API client with explicit timeouts, bounded retries, and injectable transport
- Resumable bounded schedule acquisition with explicit eligibility and per-game outcomes
- Paginated corrected-game polling with safety overlap, completed-run validation, and success-only durable watermark advancement
- Retriable corrected-game processing into immutable raw revisions
- Incremental schedule discovery with a rolling lookback and independent through-date
- Durable deferred-game worklist independent of the rolling schedule window
- Durable daily coordinator with isolated schedule, deferred-game, correction-discovery, and correction-processing outcomes
- Resumable multi-season backfill with explicit reconciliation modes, dry-run planning, monthly child manifests, and independent season checkpoints
- Tests based on representative checked-in MLB responses
- Target architecture and ADR process

## 1. Acquisition — application implementation established

- Generalize storage behavior into protocols and portable artifact references. Established in ADR 0008.
- Back the established persistence state machines with conditionally written S3 objects. Established in ADR 0009.
- Compose the same daily coordinator at a cached Lambda application boundary. Established in ADR 0009.
- Define and deploy the acquisition bucket as code. Established in ADR 0010.
- Define and deploy the Lambda execution role with prefix-scoped S3 access. Established in ADR 0011.
- Package and deploy a manually invokable Lambda plus log retention. Established in ADR 0012.
- Reconcile historical seasons from a local CLI against local or S3 storage. Established in ADR 0013.
- Define EventBridge scheduling as code. The direct Lambda target from ADR 0014
  is superseded by the workflow-owned Step Functions schedule in ADR 0017; the
  one-time CloudFormation ownership migration remains operator-controlled.
- Add alarms and a failed-event destination, then verify the first scheduled invocation.

Local exit demo established: bootstrap one daily run, acquire eligible schedule games, replay an MLB correction into a new revision, rerun safely, and inspect the schedule, correction, watermark, and coordinator manifests.

Cloud exit demo: deploy the infrastructure, run the Step Functions workflow
through S3, rerun it safely, and inspect CloudWatch status plus S3 evidence and
watermarks.

## 2. Analytical storage

- Define versioned tabular contracts. All 25 game, play/event, runner/fielding,
  player, and boxscore contracts are established in ADR 0015.
- Replace mutation-heavy generic flattening with explicit mappings for durable
  fields. Established for the complete local game projection.
- Publish local Parquet atomically with current-revision provenance. Established
  for inspection runs. Production Glue reconciliation, Iceberg v2 natural-key
  merges, a completion registry, and a queryable current-revision mapping are
  implemented in ADR 0016. The first production job completed successfully and
  its Iceberg tables are queryable in Athena.
- Add schema-drift reports, cross-game uniqueness checks, production
  referential checks, and source-to-output reconciliation. Per-game contracts,
  uniqueness, extension relationships, and event-family counts are established.

Local projection demo established: project every current local game revision
into all planned analytical datasets, validate their relationships, and inspect
explicit schemas, row counts, event-family counts, and sample rows.

Production exit demo: deploy the analytical stack, project current S3 revisions,
query all Iceberg tables in Athena, rerun with zero pending revisions, then land
a correction and observe one revision reconcile safely.

## 3. Transformation

- Audit existing dbt models and classify them as retain, rewrite, or retire.
- Establish naming, tests, documentation, lineage, and CI conventions.
- Build conformed game, team, player, plate-appearance, pitch, and runner models.
- Make local and production dbt targets behaviorally equivalent.

Exit demo: reproduce a selected player/game calculation from raw response through a tested dbt mart.

## 4. Semantics

- Inventory application metrics and their grains, filters, dimensions, and owners.
- Evaluate semantic-layer options against local operation, dbt integration, caching, API ergonomics, and deployability.
- Define a small canonical metric set before migrating every statistic.

Exit demo: query the same metric by player, team, and season without embedding metric SQL in the presentation layer.

## 5. Presentation

- Select the first portfolio narrative and user workflow.
- Rebuild the UI against semantic contracts rather than generated static data files.
- Add loading, empty, stale-data, and error states plus visual regression coverage.

Exit demo: a deployed, documented feature whose displayed value traces back to source records and metric definitions.

## 6. Operations and portfolio polish

- Define cloud infrastructure, schedules, permissions, secrets, and data retention as code.
- Add structured logs, run status, cost visibility, freshness objectives, and alerts.
- Create architecture diagrams, a public data dictionary, demo data, and a reproducible deployment guide.
- Rehearse backfills, corrections, disaster recovery, and a clean setup from a new clone.
