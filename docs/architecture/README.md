# Architecture decisions

This directory records why Zavant is built the way it is. The target architecture describes system boundaries; ADRs record decisions that are difficult or expensive to reverse.

## Decision register

| Area | Legacy choice | Current direction | Status |
|---|---|---|---|
| Migration | Replace components in place without explicit parity gates | Preserve `v1`; replace through tested `v2` vertical slices | Accepted in ADR 0001 |
| Raw persistence | S3 objects keyed by year and game ID | Content-addressed game revisions, immutable schedule and change snapshots, manifests, checksums, and provenance | Accepted in ADRs 0002–0003; shared path-backed stores implemented |
| Source contract | Implicit dictionary access | Versioned validation for live-game, schedule, and corrected-game responses | Initial contracts implemented |
| Game discovery | Mutable schedule response used only in process memory | Immutable bounded schedule snapshots and discovered-game manifests | Accepted in ADR 0003; shared path-backed store implemented |
| API client | Unbounded `requests.get` calls | Storage-neutral typed methods, explicit timeouts, bounded retries, and injectable transport | Accepted in ADR 0004; implemented |
| Initial acquisition | Schedule loop coupled to S3 bucket scans | Bounded schedule evidence, explicit eligibility, per-game outcomes, and safe local resumption | Accepted in ADR 0005; local workflow implemented |
| Game eligibility | Inline final-state and series-description checks | Named policy: acquire final regular season, defer unfinished, skip other types | Accepted in ADR 0005; implemented |
| Incremental state | Bucket scans and text tracking files | Independent correction timestamp and schedule through-date, both advanced only by successful discovery | Accepted in ADRs 0006–0007; local processing lifecycle implemented |
| Daily acquisition | One season-wide scan inside a Lambda invocation | Independent correction discovery and processing, durable deferred-game reconciliation, and rolling schedule discovery with a coordinator manifest | Accepted in ADRs 0007 and 0019; local and Lambda workflows implemented |
| Historical acquisition | Ad hoc season-wide reruns with no revision audit state | Resumable monthly schedule children, explicit reconciliation modes, and season-scoped correction checkpoints | Accepted in ADR 0013; local and S3-backed CLI implemented |
| Storage boundary | Acquisition imports concrete filesystem stores and exchanges `Path` values | Shared persistence state machines behind domain protocols, with local files or conditionally written S3 objects | Accepted in ADRs 0008–0009; local and S3 composition implemented |
| Analytical format | Flattened JSON followed by Glue-written Parquet | Explicit revision-aware datasets, locally inspectable Parquet, and production Iceberg v2 tables | Accepted in ADRs 0015–0016; local projection and production Glue publication implemented |
| Schema evolution | Implicit table shape changes | Explicit additive Iceberg migrations; breaking changes default to new physical tables; exact runtime validation remains | Accepted in ADR 0018 |
| Player game grain | One player row per game, assuming one club | Player, position, and statistic rows are keyed by both player and team to preserve legitimate dual-team appearances | Accepted in ADR 0020 |
| Player dimension | Treat the latest boxscore team as current affiliation | Build a Type 1 profile from latest game evidence and label affiliation as the most recent game team | Accepted in ADR 0023; initial model implemented |
| Current analytical state | Repeat revision-pointer joins in downstream SQL | Glue-owned `current_*` Athena views expose business grains while preserving internal Iceberg history | Accepted in ADR 0021; implemented |
| Transformation | dbt on Athena | Build business-grained staging, intermediate, and mart models over current Glue views | Accepted in ADR 0021; 25 staging models, five correction-safe facts, two dimensions, shared merge macros, and reconciliation tests implemented |
| Production orchestration | AWS Step Functions plus S3 events | A workflow-owned EventBridge schedule starts one daily Standard workflow that sequences acquisition, batch projection, and later dbt | Accepted in ADR 0017; schedule plus acquisition and projection states implemented |
| Infrastructure | Manually configured AWS services and shell deployment | Native CloudFormation with separate acquisition, analytical, workflow, and Hex integration stacks, scoped roles, packaged compute, controlled log retention, and one explicit schedule | Accepted in ADRs 0010–0012, 0014–0017, and 0022; Lambda, Glue, Step Functions, and Hex access definitions implemented |
| Semantics | Presentation-specific SQL and exported files | Source-controlled MetricFlow definitions synchronized into Hex | Accepted in ADR 0022; plate-appearance, batted-ball, pitch, runner-movement, participation, player, and team models implemented |
| Presentation | Create React App consuming generated artifacts | Hex semantic exploration, Threads, notebooks, and published data products over Athena | Accepted in ADR 0022; integration infrastructure and published player profile implemented |

## Adding an ADR

Copy `decisions/0000-template.md`, assign the next number, and include alternatives and consequences. Use an ADR for decisions that constrain later work; keep routine implementation notes in code or pull requests.
