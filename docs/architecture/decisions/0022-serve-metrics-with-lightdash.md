# ADR 0022: Serve governed metrics with Lightdash

- Status: Accepted
- Date: 2026-08-13

## Context

Zavant needs a portfolio-quality semantic and presentation layer for metrics
and BI workflows rather than a bespoke React application. MetricFlow provides a
useful source-controlled specification, but its released query runtime does not
support the Athena adapter. Reimplementing its query engine or moving the
warehouse solely for metric serving would add disproportionate complexity.

Lightdash supports Athena directly and can translate supported MetricFlow
resources from dbt's compiled manifest into Lightdash metrics. The initial
plate-appearance semantic model uses simple, ratio, and same-model derived
metrics, all of which are supported by that translation.

## Decision

Keep MetricFlow YAML as the source-controlled metric specification and use
Lightdash as the initial semantic-query and BI runtime. Lightdash executes its
own generated SQL through a dedicated Athena workgroup rather than relying on
MetricFlow's warehouse runtime. Deploy MetricFlow resources through the
Lightdash CLI, whose compile step translates them from dbt's manifest into the
Lightdash semantic layer.

Use Lightdash Cloud initially. A separate CloudFormation stack owns an
encrypted, lifecycle-managed query-results bucket, a per-query scan limit, and
a dedicated IAM user. The user can query only the production dbt database, read
only production dbt table objects, and manage objects only in its query-results
bucket. CloudFormation does not create or expose the user's long-lived access
key.

## Consequences

Metrics remain reviewable alongside dbt while Lightdash supplies exploration,
charts, dashboards, and metric-serving behavior. The existing Athena and dbt
architecture remains unchanged, and a future self-hosted Lightdash deployment
can assume a role with the same policy boundary.

Lightdash's MetricFlow translation is Beta. It does not currently translate
cumulative or conversion metrics, MetricFlow implicit entity joins,
cross-model ratio or derived metrics, or time-spine behavior. Deployment
validation must surface unsupported definitions. A Git-connected project
refresh does not replace the CLI translation step. Dimensions come from the
physical dbt model columns, and cross-model joins must be declared explicitly
for Lightdash. A `lightdash` dbt tag and matching project selector expose only
intentional presentation marts rather than the entire transformation graph.

Lightdash Cloud requires a dedicated access key. It must be created after stack
deployment, entered directly into Lightdash, kept out of project configuration,
and intentionally rotated. The separate query-results bucket avoids retaining
ephemeral outputs in the versioned acquisition bucket.

## Alternatives considered

- Query Athena through MetricFlow directly. Rejected until Athena support is
  available in a stable MetricFlow release.
- Define a second independent set of Lightdash-only metrics. Rejected because
  duplicate definitions would drift, although Lightdash-specific YAML remains
  available for features MetricFlow cannot express.
- Self-host Lightdash immediately. Deferred because running its application,
  metadata database, networking, authentication, upgrades, and backups adds
  operational surface without improving the first semantic vertical slice.
- Build a custom BI application. Rejected because it demonstrates application
  development more than analytics engineering and recreates commodity BI
  capabilities.
