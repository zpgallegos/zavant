# Zavant infrastructure

The CloudFormation stack owns the durable S3 bucket, the daily acquisition Lambda function, its execution role, its retention-controlled log group, and the EventBridge Scheduler resources that invoke it. Monitoring remains a later infrastructure slice.

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

`AcquisitionPrefix` is a stack parameter and defaults to `lake`. The deploy target passes the matching `S3_PREFIX` Make variable, keeping the Lambda's `ZAVANT_S3_PREFIX` environment value and IAM resource boundary aligned.

## Lambda boundary

- Python 3.12 runs on ARM64 with the handler `zavant.lambda_handler.lambda_handler`.
- The deployment zip contains the application and the locked Boto3 dependency tree.
- Package bytes are addressed by SHA-256 beneath `deployments/lambda/`; the execution role cannot read that deployment prefix.
- The function receives the bucket, prefix, and bootstrap boundaries directly from stack parameters and resources.
- Memory is 512 MB and timeout is 900 seconds.
- Reserved concurrency is intentionally unset so deployment works with reduced new-account quotas; the daily cadence cannot naturally overlap a 15-minute invocation.
- JSON-formatted CloudWatch logs expire after 30 days.

## Daily schedule

- EventBridge Scheduler invokes the complete Lambda workflow once a day at 6:00 AM in `America/Los_Angeles` by default.
- Scheduler handles daylight-saving transitions for the configured timezone. Both the cron expression and timezone are CloudFormation parameters.
- The schedule is enabled by default and can be deployed disabled with `DAILY_SCHEDULE_STATE=DISABLED`.
- Flexible delivery is disabled. Failed target deliveries can be retried twice for up to one hour; the application remains safe under repeated invocation because its storage transitions are conditional and its acquisition operations are idempotent.
- Scheduler receives a dedicated execution role whose trust is restricted to this AWS account and the default schedule group. Its only permission is `lambda:InvokeFunction` on the acquisition function.
- The target payload is `{}`, so the handler chooses its ordinary current-date boundary.

## Validate

Validation is read-only. The project defaults to Region `us-east-1`; credentials still come from the normal AWS CLI configuration:

```shell
make infra-validate
```

CloudFormation validation checks template syntax, not whether the caller can create every resource. Inspect the completed stack and perform a post-deployment security smoke test before writing production data.

## Deploy

The production stack follows the `{project}-{workload}-{environment}` convention and is named `zavant-acquisition-prod`:

For a brand-new stack, bootstrap the artifact bucket and role first:

```shell
make infra-bootstrap EXPECTED_AWS_ACCOUNT_ID=<12-digit-account-id>
```

Then build the zip, upload it to the generated bucket, and create or update the function:

```shell
make infra-deploy \
  EXPECTED_AWS_ACCOUNT_ID=<12-digit-account-id> \
  INITIAL_SCHEDULE_DATE=<YYYY-MM-DD> \
  INITIAL_CORRECTION_WATERMARK=<UTC-timestamp>
```

The existing production stack is already bootstrapped, so ordinary updates use the same deploy command and explicit parameters. Before deployment, the target verifies that active credentials belong to the supplied account. It builds and import-checks the package, uploads it under a content-addressed key, acknowledges generated-name IAM resources, and updates the stack in `us-east-1`.

The schedule defaults can be overridden during deployment:

```shell
make infra-deploy \
  EXPECTED_AWS_ACCOUNT_ID=<12-digit-account-id> \
  INITIAL_SCHEDULE_DATE=<YYYY-MM-DD> \
  INITIAL_CORRECTION_WATERMARK=<UTC-timestamp> \
  DAILY_SCHEDULE_EXPRESSION='cron(0 7 * * ? *)' \
  DAILY_SCHEDULE_TIMEZONE=America/Los_Angeles \
  DAILY_SCHEDULE_STATE=ENABLED
```

To retrieve the generated bucket name afterward:

```shell
aws cloudformation describe-stacks \
  --region us-east-1 \
  --stack-name zavant-acquisition-prod \
  --query 'Stacks[0].Outputs[?OutputKey==`AcquisitionBucketName`].OutputValue' \
  --output text
```

Use that output as `ZAVANT_S3_BUCKET`. Use the stack's `AcquisitionPrefix` output as `ZAVANT_S3_PREFIX`; it defaults to `lake`.

Inspect the deployed schedule with:

```shell
aws scheduler get-schedule \
  --region us-east-1 \
  --group-name default \
  --name zavant-acquisition-daily-prod
```

## Manual smoke test

The checked-in event is an empty JSON object, so the function uses the current UTC date:

```shell
make lambda-invoke
cat build/lambda-response.json
```

Override `LAMBDA_EVENT_FILE` with another JSON file containing `{"through_date":"YYYY-MM-DD"}` for a deterministic boundary. A successful invocation writes its daily manifest and any discovered source artifacts beneath `s3://<bucket>/lake/`. A function error is reported in the invocation metadata and CloudWatch log group `/aws/lambda/zavant-acquisition-daily-prod`. For a new environment, deploy with `DAILY_SCHEDULE_STATE=DISABLED` until this smoke test passes, then redeploy with the schedule enabled.

The first scheduled run should be verified in CloudWatch and against its S3 daily-run manifest. Alarms and a failed-event destination remain follow-up operational work.

Deleting the stack does not delete the bucket or bucket policy. This is intentional protection for the raw lake. The function, roles, schedule, and log group follow normal stack deletion behavior. Removing retained bucket resources requires a separate, explicit cleanup decision.
