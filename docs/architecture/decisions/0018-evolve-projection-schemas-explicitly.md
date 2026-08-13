# ADR 0018: Evolve projection schemas explicitly

- Status: Accepted
- Date: 2026-08-11

## Context

The Glue projector creates missing Iceberg tables from Python contracts and
then requires exact agreement in column name, order, Spark type, and
nullability. That protects steady-state writes from silent drift, but neither
Iceberg schema evolution nor a contract-version column can make dbt models
simultaneously support incompatible upstream schemas and meanings.

## Decision

Keep `CREATE TABLE IF NOT EXISTS` plus exact validation as the steady-state
guardrail. Treat each set of analytical Iceberg tables as one projection
generation. A projection release that changes schemas, grains, keys, or
established semantics requires a complete analytical rebuild:

1. Update the Python contracts. After the initial v1 contract is established,
   bump `PROJECTION_CONTRACT_VERSION` for each subsequent release.
2. Update dependent dbt sources and models in the same repository change.
3. Delete the analytical Iceberg tables, their data, and the legacy catalog
   entries using the reviewed operator scripts.
4. Deploy the projector, run Glue to reproject every immutable raw revision,
   and fully rebuild the affected dbt models.
5. Validate revision completeness, current-pointer integrity, and downstream
   reconciliations before treating the release as complete.

The contract version remains lineage on projected rows and manifests, but is
not part of analytical natural keys or dbt logic. The Glue job rejects an
existing `games` table containing another contract version, including
schema-compatible semantic releases that might otherwise skip reprojection.
Schema validation errors continue to identify missing, unexpected,
incompatible, or reordered columns.

## Consequences

Projection and dbt changes are deployed as one coordinated analytical release.
This is operationally heavier than in-place evolution, but it gives every table
one schema and one meaning, makes full revision history reproducible, and keeps
contract versions out of downstream grains. Exact runtime validation continues
to stop accidental drift before any projection merge occurs.

## Alternatives considered

- Automatically reconcile every contract difference. Rejected because rename,
  removal, type, nullability, and semantic changes cannot be inferred safely.
- Keep multiple contract versions as rows in the same tables. Rejected because
  incompatible schemas cannot coexist and dbt cannot safely interpret multiple
  upstream meanings at once.
- Migrate compatible changes in place. Deferred until rebuild cost or uptime
  requirements justify a more complex release and rollback mechanism.
- Disable exact validation and rely on Spark writes. Rejected because failures
  would occur later and could differ across the 25 tables.
