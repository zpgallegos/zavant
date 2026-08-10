# ADR 0011: Scope Lambda storage access to the acquisition prefix

- Status: accepted
- Date: 2026-08-09

## Context

The Lambda composition uses only four S3 operations: listing logical keys, checking or reading exact objects, and conditionally publishing objects. Giving the function broad S3 access would permit unrelated reads, destructive operations, or bucket administration that the application neither implements nor needs.

The storage prefix is runtime configuration. Hard-coding it independently into an IAM policy could break production when one value changes without the other.

## Decision

Create a CloudFormation-managed IAM role trusted only by `lambda.amazonaws.com`, with inline S3 and CloudWatch Logs policies:

- Allow `s3:ListBucket` on the acquisition bucket only when `s3:prefix` is the configured prefix or a key beneath it.
- Allow `s3:GetObject` and `s3:PutObject` only beneath that prefix.
- Grant no delete, version-management, ACL, bucket-administration, or cross-bucket actions.
- Allow stream creation and event publication only in the function's explicit log group.

Represent the prefix as the `AcquisitionPrefix` stack parameter, defaulting to `lake`. The future Lambda resource will use the same value for `ZAVANT_S3_PREFIX`. Let CloudFormation generate the role name so the template remains portable and replacement-safe.

## Alternatives considered

- `AmazonS3FullAccess` is easy to attach but grants unrelated and destructive operations across the account.
- Bucket-wide object permissions avoid a prefix condition but allow the function to cross future workload boundaries inside the bucket.
- A bucket policy naming the role could enforce the same allow rules, but an identity policy keeps application permissions with the execution identity. The bucket policy remains responsible for transport-level denial.
- A fixed role name is easier to recognize manually but introduces account-wide naming collisions and replacement constraints.

## Consequences

- The role matches the current S3 adapter's complete AWS API surface.
- A new required S3 operation must be reviewed and added explicitly rather than inheriting broad access.
- Changing the acquisition prefix through `make infra-deploy S3_PREFIX=...` updates the role policy; the future Lambda environment must consume the same stack parameter.
- CloudWatch logging is scoped to the explicit retention-controlled function log group.
- A real Lambda-to-S3 smoke test remains necessary after packaging and function deployment.
