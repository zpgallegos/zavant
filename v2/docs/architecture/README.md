# Architecture decisions

This directory records why Zavant is built the way it is. The target architecture describes system boundaries; ADRs record decisions that are difficult or expensive to reverse.

## Decision register

| Area | Legacy choice | Current direction | Status |
|---|---|---|---|
| Migration | Replace components in place without explicit parity gates | Preserve `v1`; replace through tested `v2` vertical slices | Accepted in ADR 0001 |
| Raw persistence | S3 objects keyed by year and game ID | Content-addressed game revisions, current pointers, immutable change pages, checksums, and provenance | Accepted in ADR 0002; local adapter implemented |
| Source contract | Implicit dictionary access | Versioned validation for live-game and corrected-game responses | Initial contracts implemented |
| API client | Unbounded `requests.get` calls | Explicit timeouts, retry policy, and injectable transport | Next slice |
| Incremental state | Bucket scans and text tracking files | Bounded change-feed polls, run manifests, and success-only watermarks | Change manifest implemented; processing lifecycle pending |
| Analytical format | Flattened JSON followed by Glue-written Parquet | Explicit datasets and locally testable Parquet publication | To decide in slice 2 |
| Transformation | dbt on Athena | Retain dbt; reassess engine and model design independently | Partially accepted |
| Orchestration | AWS Step Functions plus S3 events | Select after defining job/run semantics | Open |
| Semantics | Presentation-specific SQL and exported files | Central metric and entity definitions | Open |
| Presentation | Create React App consuming generated artifacts | Select from product requirements after semantics | Open |
| Infrastructure | Manually configured AWS services and shell deployment | Versioned infrastructure as code | Accepted direction |

## Adding an ADR

Copy `decisions/0000-template.md`, assign the next number, and include alternatives and consequences. Use an ADR for decisions that constrain later work; keep routine implementation notes in code or pull requests.
