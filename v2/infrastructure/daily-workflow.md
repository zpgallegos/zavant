# Daily workflow infrastructure

The `zavant-daily-workflow-prod` stack owns the Standard Step Functions state
machine, its execution role and 30-day CloudWatch log group, the EventBridge
Scheduler schedule, and Scheduler's start-execution role. It references the
acquisition Lambda and analytical Glue job without owning either compute
resource.

The scheduled state machine runs:

```text
EventBridge Scheduler
    -> Run daily acquisition Lambda
    -> wait for success
    -> start analytical Glue job with .sync
    -> wait for success
```

An acquisition failure prevents Glue from starting. A Glue failure leaves the
execution failed and the affected revisions absent from the completed
projection registry, so the next successful workflow reconciles them again.

## Daily schedule

- The default cadence is 6:00 AM in `America/Los_Angeles` every day.
- EventBridge Scheduler applies daylight-saving transitions in the configured
  timezone.
- Flexible delivery is disabled. A failed state-machine start can be retried
  twice for up to one hour.
- Scheduler's role can only call `states:StartExecution` on this state machine.
- The `{}` input is passed through to Lambda, selecting the ordinary current-date
  acquisition boundary.

The expression, timezone, and state are required values supplied from
`ZAVANT_DAILY_SCHEDULE_*` in `.env`; the CloudFormation template has no fallback
defaults. Set `ZAVANT_DAILY_SCHEDULE_STATE=DISABLED` before deployment when the
workflow should be verified manually before automatic execution.

## Validate and deploy

```shell
make workflow-infra-validate
make workflow-infra-deploy
```

Deployment reads the Lambda ARN and Glue job name from the existing
`zavant-acquisition-prod` and `zavant-analytics-prod` stack outputs. It then
creates or updates the state machine and its schedule in one workflow-owned
stack.

## One-time ownership migration

CloudFormation cannot transfer a named resource directly between stacks. For an
environment previously deployed from the acquisition-owned schedule template,
run these commands in order:

```shell
make acquisition-infra-deploy
make workflow-infra-deploy
```

The first deployment removes `zavant-acquisition-daily-prod` from the
acquisition stack. The second recreates that same physical schedule under the
workflow stack. This produces a brief unscheduled gap and never leaves two
active daily schedules.

## Manual verification

Start the complete workflow independently of its schedule:

```shell
make workflow-start
```

The default input is the checked-in empty JSON event. Override
`DAILY_WORKFLOW_INPUT_FILE` with a JSON file containing
`{"through_date":"YYYY-MM-DD"}` for a deterministic acquisition boundary. Use
the returned execution ARN with `aws stepfunctions describe-execution` and
confirm that both states succeeded.

Inspect the deployed schedule with:

```shell
aws scheduler get-schedule \
  --region us-east-1 \
  --group-name default \
  --name zavant-acquisition-daily-prod
```

The first scheduled run should be verified in Step Functions, CloudWatch, and
its S3 daily-run manifest. Alarms and a failed-event destination remain
follow-up operational work.
