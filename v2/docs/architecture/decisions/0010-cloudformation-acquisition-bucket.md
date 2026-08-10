# ADR 0010: Provision the acquisition bucket with CloudFormation

- Status: accepted
- Date: 2026-08-09

## Context

The S3 adapter requires durable object storage, but the repository had no infrastructure tool or repeatable bucket definition. The bucket contains immutable source evidence as well as mutable manifests, pointers, and watermarks. Accidental public access, stack deletion, or an unconditional resource replacement could therefore destroy both analytical history and operational recovery state.

This slice provisions only the bucket. Lambda packaging, execution permissions, scheduling, and monitoring have different lifecycles and remain subsequent work.

## Decision

Use a native CloudFormation template as the initial infrastructure boundary. CloudFormation is available through the existing AWS CLI, introduces no project runtime dependency, and can own the later Lambda and EventBridge resources in the same stack.

Name the production stack `zavant-acquisition-prod`, following the `{project}-{workload}-{environment}` convention. Let CloudFormation generate the physical bucket name because S3 names are globally unique. Export the generated name and ARN for later application configuration and least-privilege IAM policies.

Configure the bucket with:

- S3-managed server-side encryption.
- Versioning for recovery from accidental overwrites or deletes.
- Bucket-owner-enforced object ownership with ACLs disabled.
- Every S3 public-access block enabled.
- A bucket policy denying requests that do not use TLS.
- Cleanup of incomplete multipart uploads after seven days.
- Project, component, environment, and management tags.
- `Retain` deletion and replacement policies on both the bucket and its policy.

Do not expire current or noncurrent object versions yet. Version expiration is irreversible and should follow an explicit recovery and cost-retention decision rather than being bundled into initial provisioning.

## Alternatives considered

- Terraform would provide a strong multi-cloud ecosystem but introduces state storage, provider versioning, and a new CLI before the project needs them.
- AWS CDK would allow Python infrastructure, but adds synthesis dependencies and abstraction for a single native resource.
- Manual bucket creation would make security controls and recovery behavior difficult to reproduce or review.
- A fixed bucket name would make the template less portable and could fail because bucket names are global.
- Deleting noncurrent versions immediately would reduce storage but remove a useful recovery mechanism before its requirements are understood.

## Consequences

- Bucket configuration is reviewable, repeatable, and extendable to the remaining AWS resources.
- Stack deletion intentionally leaves a retained bucket and policy that require explicit cleanup.
- Deployments must capture the generated bucket output for `ZAVANT_S3_BUCKET` until the Lambda resource consumes it directly.
- Version storage can grow and should be measured before a lifecycle policy is selected.
- CloudFormation syntax validation does not replace a real deployment and post-deployment security smoke test.
