# ADR 0012: Package and manually verify the daily Lambda before scheduling

- Status: accepted
- Date: 2026-08-09

## Context

The daily composition boundary is implemented and unit-tested, but CloudFormation cannot create a zip-based Lambda directly from repository files. A deployment process must package application code and dependencies, publish immutable code bytes, and connect the function to its existing bucket and execution role.

Enabling a schedule at the same time would allow an unverified package, IAM policy, or real-S3 behavior to run automatically.

## Decision

Build a zip containing Zavant and a locked, application-owned Boto3 dependency tree. Import-check the packaged handler, hash the zip with SHA-256, and upload it beneath `deployments/lambda/<sha256>.zip` in the acquisition bucket. Pass that key to CloudFormation so a byte change produces an explicit function code update.

Run `zavant.lambda_handler.lambda_handler` on Python 3.12 and ARM64 with 512 MB of memory and a 900-second timeout. Do not reserve concurrency: reduced-quota AWS accounts must retain their small concurrency allocation in the unreserved pool, while a daily schedule cannot naturally overlap a 15-minute invocation. Conditional S3 publication detects accidental competing writers rather than silently losing manifest or watermark state.

Supply the generated bucket name, shared acquisition prefix, and bootstrap boundaries as environment variables from the stack. Create the function's log group explicitly with JSON logs and 30-day retention. Restrict the execution role to publishing in that log group rather than using account-wide log resources.

Do not create an EventBridge rule or invoke permission yet. First deploy and invoke the function manually, inspect its response, logs, S3 evidence, and watermarks, and confirm that an immediate rerun is safe.

## Alternatives considered

- Rely on runtime-bundled Boto3. This makes dependency versions change with runtime updates and can create SDK dependency skew.
- Use a Lambda layer. One function does not yet justify a second independently versioned deployment artifact.
- Use a container image. The application has no native dependency or system-package requirement that offsets ECR and image-build overhead.
- Enable EventBridge immediately. This reduces a deployment step but makes production data and recurring costs depend on an unverified integration.
- Reserve one concurrent execution. This would express the single-run preference directly, but accounts with the reduced 10-execution quota cannot reserve capacity because AWS requires the allocation to remain unreserved.

## Consequences

- The same declared dependency versions and application package are reviewed and uploaded together.
- Code artifacts accumulate in the retained bucket; a lifecycle policy can be added after rollback requirements are established.
- A brand-new environment requires a foundation deployment before code can be uploaded to the generated bucket.
- Operators should not invoke the daily function concurrently. An accidental overlap may fail one invocation through storage conflict detection and can then be retried safely.
- The maximum execution window is explicit. If daily volume approaches it, evidence should drive batching or orchestration changes rather than silently raising the limit.
- EventBridge, asynchronous failure handling, alarms, and cost monitoring remain separate changes after the manual smoke test.
