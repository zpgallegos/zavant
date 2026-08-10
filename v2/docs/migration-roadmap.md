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
- Durable daily coordinator with isolated schedule, correction-discovery, and correction-processing outcomes
- Tests based on representative checked-in MLB responses
- Target architecture and ADR process

## 1. Acquisition — local workflow established

- Generalize local storage behavior into protocols before adding cloud adapters.
- Add an S3 adapter and contract tests shared with the local adapter.

Local exit demo established: bootstrap one daily run, acquire eligible schedule games, replay an MLB correction into a new revision, rerun safely, and inspect the schedule, correction, watermark, and coordinator manifests.

Cloud exit demo: run the same workflow through storage protocols backed by S3 and verify behavioral parity with the local adapters.

## 2. Analytical storage

- Define versioned tabular contracts for games, teams, players, plays, events, runners, and box scores.
- Replace mutation-heavy generic flattening with explicit mappings for durable fields.
- Publish Parquet atomically and design a correction/backfill strategy.
- Add schema-drift reports, uniqueness checks, referential checks, and source-to-output reconciliation.

Exit demo: convert one raw game into validated Parquet and query every dataset locally.

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
