# ADR 0022: Serve governed metrics with Hex

- Status: Accepted
- Date: 2026-08-15

## Context

Zavant needs a portfolio-quality semantic and presentation layer for metrics
and BI workflows rather than a bespoke React application. MetricFlow provides a
useful source-controlled specification, but its released query runtime does not
support the Athena adapter. Reimplementing its query engine or moving the
warehouse solely for metric serving would add disproportionate complexity.

Hex connects directly to Athena, imports dbt MetricFlow projects, supports
interactive semantic exploration and agent-assisted analysis, and can publish
notebooks and data products without requiring a custom application.

## Decision

Keep MetricFlow YAML as the source-controlled metric specification and use Hex
as the semantic exploration and presentation runtime. Synchronize semantic
resources through the checked-in GitHub Actions context workflow, which previews
changes before publishing them to the configured Hex semantic project.

A separate CloudFormation stack owns an encrypted, lifecycle-managed Athena
query-results bucket, a per-query scan limit, a dedicated workgroup, and a
read-only IAM role. Hex assumes the role with temporary credentials and an
external ID. The role can query only the production dbt database, read only
production dbt table objects, and manage objects only in its query-results
bucket.

## Consequences

Metrics remain reviewable alongside dbt while Hex supplies semantic exploration,
Threads, notebooks, charts, and published presentation experiences. The Athena
and dbt architecture remains unchanged, and Hex receives no long-lived AWS
credentials.

Production dbt relations must exist and the Hex Athena schema must be refreshed
before a newly synchronized semantic model can infer its columns. Semantic
changes are not published merely by refreshing the connection: the context
workflow must also run. The per-query scan cutoff limits one query rather than
aggregate daily spend. Published access therefore remains bounded by the Athena
workgroup cutoff and Hex's execution and caching controls rather than being
treated as an unlimited query endpoint.

## Alternatives considered

- Query Athena through MetricFlow directly. Rejected until Athena support is
  available in a stable MetricFlow release.
- Build a custom BI application. Rejected because it demonstrates application
  development more than analytics engineering and recreates commodity BI
  capabilities.
- Move the warehouse to match a supported semantic-query runtime. Rejected
  because presentation requirements do not justify replacing the established
  S3, Iceberg, Glue, and Athena platform.
