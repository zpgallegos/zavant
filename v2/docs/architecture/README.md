# Architecture decisions

This directory records why Zavant is built the way it is. The target architecture describes system boundaries; ADRs record decisions that are difficult or expensive to reverse.

## Decision register

| Area | Legacy choice | Current direction | Status |
|---|---|---|---|
| Migration | Replace components in place without explicit parity gates | Preserve `v1`; replace through tested `v2` vertical slices | Accepted in ADR 0001 |
| Raw persistence | S3 objects keyed by year and game ID | Content-addressed game revisions, immutable schedule and change snapshots, manifests, checksums, and provenance | Accepted in ADRs 0002–0003; local adapters implemented |
| Source contract | Implicit dictionary access | Versioned validation for live-game, schedule, and corrected-game responses | Initial contracts implemented |
| Game discovery | Mutable schedule response used only in process memory | Immutable bounded schedule snapshots and discovered-game manifests | Accepted in ADR 0003; local adapter implemented |
| API client | Unbounded `requests.get` calls | Storage-neutral typed methods, explicit timeouts, bounded retries, and injectable transport | Accepted in ADR 0004; implemented |
| Initial acquisition | Schedule loop coupled to S3 bucket scans | Bounded schedule evidence, explicit eligibility, per-game outcomes, and safe local resumption | Accepted in ADR 0005; local workflow implemented |
| Game eligibility | Inline final-state and series-description checks | Named policy: acquire final regular season, defer unfinished, skip other types | Accepted in ADR 0005; implemented |
| Incremental state | Bucket scans and text tracking files | Independent correction timestamp and schedule through-date, both advanced only by successful discovery | Accepted in ADRs 0006–0007; local processing lifecycle implemented |
| Daily acquisition | One season-wide scan inside a Lambda invocation | Independent correction discovery, correction processing, and rolling schedule discovery with a durable coordinator manifest | Accepted in ADR 0007; local workflow implemented |
| Analytical format | Flattened JSON followed by Glue-written Parquet | Explicit datasets and locally testable Parquet publication | To decide in slice 2 |
| Transformation | dbt on Athena | Retain dbt; reassess engine and model design independently | Partially accepted |
| Production orchestration | AWS Step Functions plus S3 events | Select a scheduler/runtime around the established daily run contract | Open; local coordinator implemented |
| Semantics | Presentation-specific SQL and exported files | Central metric and entity definitions | Open |
| Presentation | Create React App consuming generated artifacts | Select from product requirements after semantics | Open |
| Infrastructure | Manually configured AWS services and shell deployment | Versioned infrastructure as code | Accepted direction |

## Adding an ADR

Copy `decisions/0000-template.md`, assign the next number, and include alternatives and consequences. Use an ADR for decisions that constrain later work; keep routine implementation notes in code or pull requests.
