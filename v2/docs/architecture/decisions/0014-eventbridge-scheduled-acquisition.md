# ADR 0014: Invoke daily acquisition with EventBridge Scheduler

- Status: Accepted
- Date: 2026-08-10

## Context

The production Lambda already composes the complete daily workflow and has been
manually invokable. Production operation needs one inexpensive invocation each
day without recreating orchestration in a cloud-specific application layer.
AWS now recommends EventBridge Scheduler instead of legacy scheduled rules.

## Decision

Define an `AWS::Scheduler::Schedule` in the acquisition CloudFormation stack.
It invokes the existing Lambda with an empty JSON event at 6:00 AM in
`America/Los_Angeles` by default. The expression, timezone, and enabled state
are stack parameters, so schedule changes do not require application releases.
Using a named timezone keeps the intended local hour stable across daylight-
saving changes.

Flexible delivery is disabled. Scheduler may retry a failed target delivery
twice for up to one hour. A dedicated execution role can invoke only the daily
Lambda, and its trust policy is restricted to the owning account and default
Scheduler group. The schedule and role exist only when Lambda code is deployed.

## Consequences

Local and manual execution continue to use the same handler and coordinator as
scheduled operation. At-least-once delivery is compatible with conditional
watermarks, immutable evidence, and idempotent landing. A rare duplicate can
still produce a visibly failed competing invocation, but it cannot silently
overwrite state.

Scheduler delivery retries do not replace operational monitoring. CloudWatch
alarms and a failed-event destination remain separate follow-up work.
