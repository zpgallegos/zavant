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
| Transformation | dbt on Athena | Retain dbt; reassess engine and model design independently | Partially accepted |
| Production orchestration | AWS Step Functions plus S3 events | A workflow-owned EventBridge schedule starts one daily Standard workflow that sequences acquisition, batch projection, and later dbt | Accepted in ADR 0017; schedule plus acquisition and projection states implemented |
| Infrastructure | Manually configured AWS services and shell deployment | Native CloudFormation with separate acquisition, analytical, and workflow stacks, scoped roles, packaged compute, controlled log retention, and one explicit schedule | Accepted in ADRs 0010–0012 and 0014–0017; Lambda, Glue, and Step Functions definitions implemented |
| Semantics | Presentation-specific SQL and exported files | Central metric and entity definitions | Open |
| Presentation | Create React App consuming generated artifacts | Select from product requirements after semantics | Open |

## Adding an ADR

Copy `decisions/0000-template.md`, assign the next number, and include alternatives and consequences. Use an ADR for decisions that constrain later work; keep routine implementation notes in code or pull requests.
