# ADR 0018: Evolve projection schemas explicitly

- Status: Accepted
- Date: 2026-08-11

## Context

The Glue projector creates missing Iceberg tables from Python contracts and
then requires exact agreement in column name, order, Spark type, and
nullability. That protects steady-state writes from silent drift, but changing
the projection contract does not alter an existing Iceberg table. The
projection contract version controls which raw revisions are processed; it is
not a physical-schema migration mechanism.

## Decision

Keep `CREATE TABLE IF NOT EXISTS` plus exact validation as the steady-state
guardrail. Treat physical schema evolution as a reviewed deployment operation:

1. Additive changes may append nullable columns. Apply an explicit, idempotent
   Iceberg migration to each deployed environment before publishing projector
   code that expects those columns.
2. Renames, removals, required columns, type changes, grain changes, and
   semantic reinterpretations are breaking by default. Publish a new physical
   table and migrate consumers deliberately unless a reviewed in-place
   migration proves compatibility.
3. Bump `PROJECTION_CONTRACT_VERSION` whenever projected values, keys, or
   schemas observably change. A migration without the version bump updates the
   table but does not reproject already registered current revisions.
4. Deploy in this order: apply and verify the physical migration, deploy the
   new projector package and contract version, run reconciliation, and validate
   row counts plus current-revision uniqueness before promoting consumers.

Schema validation errors must identify missing, unexpected, incompatible, or
reordered columns so a failed deployment points directly to the required
migration. Migration SQL belongs in a dedicated reviewed change when the first
schema change is introduced; no generic migration framework is justified yet.

## Consequences

Intentional schema changes require an explicit two-phase deployment instead of
being inferred from application code. This prevents a projector deployment
from silently mutating production tables or treating a contract-version bump
as a schema migration.

Additive evolution remains small and auditable. Breaking changes retain a clean
rollback path through a separate table. Exact runtime validation continues to
stop accidental drift before any projection merge occurs.

## Alternatives considered

- Automatically reconcile every contract difference. Rejected because rename,
  removal, type, nullability, and semantic changes cannot be inferred safely.
- Tie physical migrations to the projection contract version. Rejected because
  processing identity and table evolution have different ordering and rollback
  requirements.
- Disable exact validation and rely on Spark writes. Rejected because failures
  would occur later and could differ across the 25 tables.
