# Zavant infrastructure

The CloudFormation stack owns the durable S3 bucket, the daily acquisition Lambda function, its execution role, and its retention-controlled log group. EventBridge scheduling and monitoring remain later infrastructure slices; the function is manual-only until its real-S3 smoke test passes.

## Bucket guarantees

- CloudFormation generates the globally unique physical name and returns it as `AcquisitionBucketName`.
- All four S3 public-access blocks are enabled and ACLs are disabled.
- New objects use S3-managed server-side encryption.
- Versioning preserves earlier manifest, pointer, and watermark values for recovery.
- Incomplete multipart uploads are aborted after seven days.
- The bucket and its TLS-enforcement policy are retained if the stack is deleted or the resource is replaced.
- No lifecycle rule deletes current objects or noncurrent versions. Retention can be added after recovery requirements are explicit.

## Execution-role boundary

- Only the Lambda service can assume the role.
- Log delivery is restricted to this function's explicit CloudWatch log group.
- `s3:ListBucket` is restricted to the configured `lake` prefix.
- `s3:GetObject` and `s3:PutObject` are restricted to objects beneath `lake/` in this bucket.
- The application cannot delete objects or versions, inspect other prefixes, change bucket configuration, or manage ACLs.

`AcquisitionPrefix` is a stack parameter and defaults to `lake`. The deploy target passes the matching `S3_PREFIX` Make variable, keeping the future `ZAVANT_S3_PREFIX` environment value and IAM resource boundary aligned.

## Lambda boundary

- Python 3.12 runs on ARM64 with the handler `zavant.lambda_handler.lambda_handler`.
- The deployment zip contains the application and the locked Boto3 dependency tree.
- Package bytes are addressed by SHA-256 beneath `deployments/lambda/`; the execution role cannot read that deployment prefix.
- The function receives the bucket, prefix, and bootstrap boundaries directly from stack parameters and resources.
- Memory is 512 MB and timeout is 900 seconds.
- Reserved concurrency is intentionally unset so deployment works with reduced new-account quotas; the future daily schedule cannot naturally overlap a 15-minute invocation.
- JSON-formatted CloudWatch logs expire after 30 days.
- No EventBridge rule or Lambda invoke permission exists yet.

## Validate

Validation is read-only. The project defaults to account `995283862400` and Region `us-east-1`; credentials still come from the normal AWS CLI configuration:

```shell
make infra-validate
```

CloudFormation validation checks template syntax, not whether the caller can create every resource. Inspect the completed stack and perform a post-deployment security smoke test before writing production data.

## Deploy

The production stack follows the `{project}-{workload}-{environment}` convention and is named `zavant-acquisition-prod`:

For a brand-new stack, bootstrap the artifact bucket and role first:

```shell
make infra-bootstrap
```

Then build the zip, upload it to the generated bucket, and create or update the function:

```shell
make infra-deploy
```

The existing production stack is already bootstrapped, so ordinary updates need only `make infra-deploy`. Before deployment, the target verifies that active credentials belong to account `995283862400`. It builds and import-checks the package, uploads it under a content-addressed key, acknowledges generated-name IAM resources, and updates the stack in `us-east-1`.

To retrieve the generated bucket name afterward:

```shell
aws cloudformation describe-stacks \
  --region us-east-1 \
  --stack-name zavant-acquisition-prod \
  --query 'Stacks[0].Outputs[?OutputKey==`AcquisitionBucketName`].OutputValue' \
  --output text
```

Use that output as `ZAVANT_S3_BUCKET`. Use the stack's `AcquisitionPrefix` output as `ZAVANT_S3_PREFIX`; it defaults to `lake`.

## Manual smoke test

The checked-in event is an empty JSON object, so the function uses the current UTC date:

```shell
make lambda-invoke
cat build/lambda-response.json
```

Override `LAMBDA_EVENT_FILE` with another JSON file containing `{"through_date":"YYYY-MM-DD"}` for a deterministic boundary. A successful invocation writes its daily manifest and any discovered source artifacts beneath `s3://<bucket>/lake/`. A function error is reported in the invocation metadata and CloudWatch log group `/aws/lambda/zavant-acquisition-daily-prod`; inspect both before enabling a schedule.

Deleting the stack does not delete the bucket or bucket policy. This is intentional protection for the raw lake. The function, role, and log group follow normal stack deletion behavior. Removing retained bucket resources requires a separate, explicit cleanup decision.
