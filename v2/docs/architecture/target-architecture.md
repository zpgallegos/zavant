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

Acquisition includes a correction loop. A bounded poll of MLB's corrected-game feed produces immutable response pages and a run manifest. Each changed game is then retrieved from its complete live-feed link and landed as a new content-addressed revision when its meaningful JSON content differs. The prior revision remains available, while a small pointer identifies the revision downstream processing should currently use.

The same logical boundaries must work locally and in the cloud. Local development uses the filesystem first; production storage will use S3 behind the same interface. Cloud services must not be required to run unit tests or exercise a representative vertical slice.

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
- dbt remains the SQL transformation framework unless migration evidence shows it is a poor fit.
- Raw responses are immutable and retain their original bytes.
- Complete games are content-addressed by canonical JSON and retain all observed revisions.
- Corrected-game polls retain their page responses and deduplicate work in a run manifest.
- Object paths are deterministic and include named partitions.
- Every landed object has provenance metadata and a checksum.
- Local adapters are first-class, not mocks of the cloud implementation.
- Infrastructure will be defined as code before a new production deployment.

The orchestrator, production query engine, semantic-layer implementation, and presentation framework remain deliberate decision points. They should be selected with a thin vertical slice and recorded as ADRs rather than inherited accidentally from the legacy stack.

## Environments

- **Test:** temporary isolated storage and recorded fixtures; no network or cloud credentials.
- **Local:** `.local/` data, developer-selected dates or games, and the same contracts used in production.
- **Production:** managed object storage, scheduled orchestration, catalog/query engine, secrets management, and centralized telemetry.

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
