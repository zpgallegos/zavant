# ADR 0017: Orchestrate the daily cloud workflow with Step Functions

- Status: Accepted
- Date: 2026-08-10

## Context

EventBridge Scheduler originally invoked the acquisition Lambda directly. The
production analytical projector is now an independently reconciling Glue job
that must run after acquisition has had its daily opportunity. Two unrelated
schedules would permit projection to race acquisition and would not provide one
durable status for the daily cloud workflow.

The Glue job is safe to rerun because it reconciles immutable raw revisions
against the terminal `games` table. Acquisition is also safe under repeated delivery,
but neither component should become responsible for starting or monitoring the
other.

## Decision

Define a Standard Step Functions state machine in a separate
`zavant-daily-workflow-{environment}` CloudFormation stack. It invokes the
existing acquisition Lambda and then starts the existing analytical Glue job
with the `.sync` integration so the state machine waits for projection to
finish. An acquisition error is retained in workflow state rather than ending
execution immediately. Projection still runs because it reconciles all durable
current pointers and does not consume the acquisition response. After a
successful projection, the workflow reports the retained acquisition failure;
the overall execution therefore remains red and alertable.

The state-machine input is passed directly to Lambda, preserving manual
`through_date` execution. Lambda service and throttling failures receive a small
bounded retry; application failures are not automatically repeated. Glue job
failures are also surfaced without an immediate retry so deterministic mapping
or contract problems do not consume another job run before inspection.

The workflow role can invoke only the acquisition function. Glue job lifecycle
actions use `Resource: "*"` because the optimized Glue integration does not
support resource-scoped authorization for these calls. Workflow execution data
is logged for 30 days.

Own EventBridge Scheduler and its execution role in the daily workflow stack.
The role receives only `states:StartExecution` on the specific state machine.
The acquisition stack has no workflow ARN parameter or orchestration resources;
it only publishes the Lambda ARN consumed by the workflow deployment. Schedule
expression, timezone, and enabled state are workflow-stack parameters.

Keep the existing physical schedule name during the ownership migration.
CloudFormation cannot transfer a named resource between stacks, so an existing
environment first deploys acquisition without the old schedule and then deploys
the workflow stack to recreate it. The ordered transition creates a brief gap
without risking two active schedules. New environments follow the linear order
acquisition, analytical projection, then workflow and schedule.

## Consequences

One execution history now records acquisition and analytical publication.
Acquisition failure does not strand revisions successfully committed by another
acquisition branch. Projection failure leaves raw data safe and incomplete
revisions are retried by a later workflow. A partial acquisition remains an
overall workflow failure even when projection succeeds.
Future dbt execution can be added as another state without coupling it to either
application.

The infrastructure dependency now runs in one direction: the workflow stack
references acquisition and analytical outputs, while neither compute stack
references orchestration. Acquisition updates no longer require resolving or
passing a workflow ARN.

Standard Step Functions transitions and CloudWatch logs add a small operational
cost. The state machine does not create a cross-table transaction; the terminal
`games` merge remains the publication gate.

## Alternatives considered

- Give acquisition and projection independent schedules. Rejected because they
  can race and have no aggregate outcome.
- Have Lambda invoke Glue directly. Rejected because orchestration, permissions,
  and long-running job monitoring do not belong in acquisition code.
- Trigger projection for each landed S3 object. Rejected because corrections and
  backfills would create concurrent Iceberg commits and small files.
- Retain Scheduler in the acquisition stack. Rejected after the initial cutover
  because it gives acquisition ownership of orchestration and introduces a
  workflow-ARN back-reference plus a second acquisition deployment.
