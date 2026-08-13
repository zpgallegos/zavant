# ADR 0021: Publish current analytical views from Glue

- Status: Accepted
- Date: 2026-08-13

## Context

The Iceberg projection tables retain every immutable MLB game revision for
auditability and retry-safe publication. Requiring every dbt model to join the
same current-revision pointer exposes storage mechanics throughout the modeling
layer, expands every business grain, and makes correction handling easy to
apply inconsistently.

## Decision

Keep the revision-aware Iceberg tables as Glue-owned internal history. After
reconciling those tables, the Glue job creates or replaces one ordinary Athena
view named `current_<table>` for each projected dataset. Every view joins its
history table to `current_game_revisions` on `game_pk` and
`source_revision_id`, uses an explicit column list, and excludes revision,
projection-run, contract-version, and raw-object lineage fields.

The dbt source names remain the business dataset names but use `identifier` to
resolve to these physical views. Staging and downstream models therefore use
business grains such as `(game_pk, at_bat_index)` and never select the revision
pointer. `current_games` additionally exposes `reconciled_at` only for source
freshness evaluation.

View publication must succeed before Glue advances `current_game_revisions`.
The views are logical and dynamically read that mapping, so the mapping's
Iceberg merge becomes the current-state publication boundary. A failed view DDL
leaves the prior current mapping intact.

## Consequences

Corrections become a Glue concern: after a successful projection, every dbt
query sees the newly current game without repeating revision-selection logic.
Physical history remains available to platform operators, but it is not part of
the dbt source contract. dbt current-state tables must replace or merge corrected
business keys rather than append immutable revision facts.

The Glue role requires narrowly scoped Athena query permissions and writes DDL
query metadata under the analytical S3 prefix. A content fingerprint under the
analytical control prefix avoids view DDL when all expected catalog views exist
and their generated definitions are unchanged. A definition change or missing
view republishes the complete set. Ordinary
Athena views are intentionally used instead of Glue multi-dialect views because
Athena is the only current consumer and Lake Formation view administration
would add complexity without a present requirement.

A projection release that changes the public columns still requires coordinated
projection and dbt changes under ADR 0018. The view boundary hides source
revision evolution, not upstream schema changes.

## Alternatives considered

- Join `current_game_revisions` in every dbt staging model. Rejected because it
  duplicates platform logic and exposes revision identifiers downstream.
- Destructively overwrite current Iceberg tables. Rejected because it discards
  useful history and complicates partial-failure recovery.
- Publish Glue Data Catalog multi-dialect views. Deferred until a non-Athena
  engine needs the same governed view definitions.
