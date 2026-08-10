# ADR 0009: Run daily acquisition in Lambda over conditional S3 storage

- Status: accepted
- Date: 2026-08-09

## Context

The local daily workflow already owns the difficult persistence behavior: immutable source evidence, content-addressed game revisions, resumable manifests, correction-work discovery, and success-only watermarks. Reimplementing those state machines once per storage backend would create two acquisition products that could drift. Production also needs one explicit boundary that turns environment configuration and AWS clients into the same daily coordinator used locally.

S3 does not provide filesystem rename. Mutable pointers, manifests, and watermarks therefore need optimistic concurrency rather than the local adapter's temporary-file replacement.

## Decision

One small S3 object backend supplies exact reads, paginated prefix listing, portable `s3://` artifact URIs, and conditional publication beneath a configured bucket and prefix. The established persistence state machines use that object surface through a path facade, so their JSON contracts, logical keys, validation, idempotency, and state transitions are shared with local storage.

Object writes remember the last observed ETag. Creating an absent object uses `If-None-Match: *`; updating an observed object uses `If-Match`. A failed precondition is accepted only when the competing object's exact bytes equal the intended bytes. Otherwise it becomes an explicit write conflict. This makes immutable duplicate publication idempotent while preventing stale mutable state from silently winning.

`zavant.lambda_handler.lambda_handler` is the production composition boundary. It builds the MLB client, S3 storage bundle, and daily coordinator from environment variables and caches the composed application at module scope for warm-runtime reuse. The event may supply only a deterministic `through_date` override; ordinary EventBridge fields are ignored.

Bootstrap values are passed only when the corresponding S3 watermark does not exist. A daily result containing any failed branch raises after its diagnostic run manifest is durable, causing Lambda to report a failed invocation and making platform retries and alarms meaningful.

The deployment package declares Boto3 rather than relying on the runtime's bundled SDK. Bucket creation, versioning, lifecycle, IAM, schedule, timeout, alarms, and deployment packaging remain infrastructure-as-code work rather than being hidden in application startup.

## Alternatives considered

- Copy the path-backed stores into separate S3 stores. This duplicates the highest-risk state-transition logic and makes parity a continuing maintenance obligation.
- Download S3 objects to `/tmp` and upload the resulting tree. This weakens concurrency control, wastes Lambda disk and transfer, and obscures which object caused a failure.
- Use unconditional `PutObject` for mutable JSON. Concurrent invocations could lose watermark or manifest transitions without evidence.
- Initialize AWS clients on every handler call. This discards Lambda execution-environment reuse and adds avoidable invocation overhead.
- Create the bucket from application code. Infrastructure ownership and permissions would become implicit and difficult to reproduce.

## Consequences

- Local and Lambda entry points share contracts, acquisition services, persistence state machines, storage protocols, and coordinator composition.
- S3 preserves the exact local logical key layout beneath a configurable physical prefix.
- Unit tests cover pagination, conditional conflicts, concurrent identical writes, S3 raw revisioning, and a complete empty daily Lambda run without AWS credentials.
- A real-bucket integration test is still required before deployment because an in-memory SDK double cannot prove IAM, S3 service behavior, or packaging.
- The compact path facade is intentionally private to persistence composition; acquisition continues to exchange only storage protocols and artifact references.
