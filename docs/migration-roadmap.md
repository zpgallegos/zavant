# Migration roadmap

This records the independently demonstrable vertical slices used to replace the
earlier implementation without a big-bang rewrite.

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
- Publish local Parquet atomically with revision provenance. Established for
  inspection runs. Production Glue reconciliation, Iceberg v2 natural-key
  merges, a terminal `games` completion marker, and a queryable current-revision
  mapping are implemented in ADR 0016. The first production job completed
  successfully and its Iceberg tables are queryable in Athena.
- Add schema-drift reports, cross-game uniqueness checks, production
  referential checks, and source-to-output reconciliation. Per-game contracts,
  uniqueness, extension relationships, and event-family counts are established.

Local projection demo established: project every local game revision
into all planned analytical datasets, validate their relationships, and inspect
explicit schemas, row counts, event-family counts, and sample rows.

Production exit demo: deploy the analytical stack, project all S3 revisions,
query all Iceberg tables in Athena, rerun with zero pending revisions, then land
a correction and observe one revision reconcile safely.

## 3. Transformation — established

- Document one current-state staging view for each analytical dataset.
- Build correction-safe plate-appearance, batted-ball, pitch, runner-movement,
  and player-game participation facts plus conformed player and team dimensions.
- Centralize the repeated changed-game and tombstone invariants in documented
  dbt macros.
- Enforce fact grains, current revisions, source reconciliation, relationships,
  documentation, SQL style, and semantic parsing in the quality loop.

Exit demo established: reproduce player-season batting, contact, pitch,
baserunning, and participation calculations from retained events through tested
dbt marts.

## 4. Semantics — established

- Keep MetricFlow YAML as the source-controlled metric contract.
- Define entities, shared player and team dimensions, additive measures, and
  regrouping-safe ratios for every implemented business fact.
- Preview and publish semantic changes to Hex through the checked-in context
  synchronization workflow.

Exit demo established: query governed metrics by player, team, season, matchup,
count, pitch family, and game context without embedding metric SQL in charts.

## 5. Presentation — established first product

- Publish a filterable Hex player profile over the governed semantic project.
- Present traditional batting and contact-quality metrics with links to the
  source-controlled methodology and external Baseball Savant comparisons.
- Expand the existing product with the implemented pitch and baserunning
  surfaces rather than creating another metric definition layer.

Exit demo established: [open the published Hex player profile](https://app.hex.tech/01a00124-662e-7369-982a-ba58e4f2a22f/app/0347rXGBaRqd4gD8KHxHRr/latest).

## 6. Operations and portfolio polish — partially established

- CloudFormation owns schedules, permissions, temporary Hex access, query-cost
  controls, and data retention.
- Monitoring scripts report acquisition, Glue, and warehouse completeness;
  workflow alarms remain a follow-on operational layer.
- Recruiter-facing architecture, semantic-layer, methodology, and live-product
  links are maintained alongside the implementation.
