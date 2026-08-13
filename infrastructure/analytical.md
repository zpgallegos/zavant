# Analytical projection infrastructure

The `zavant-analytics-prod` stack owns the Glue Data Catalog database, the Glue
5.0 Spark projection job, and its least-privilege execution role. It references
the existing retained acquisition bucket but does not own or replace it.

The job reads immutable raw-game revisions and current pointers beneath
`lake/raw/`, then manages Iceberg data and metadata beneath
`lake/analytical/iceberg/`. Deployment artifacts are read from
`deployments/glue/`. The role has no access to acquisition watermarks or run
state and cannot modify raw objects.

## Validate and deploy

```shell
make analytics-infra-validate

make analytics-infra-deploy
```

Deployment retrieves the acquisition bucket and prefix from
`zavant-acquisition-prod`, builds a dependency-free Python library zip, uploads
content-addressed script and library artifacts, and deploys
`infrastructure/analytical-projection-stack.yaml` as `zavant-analytics-prod`.

The deployed job uses two G.1X workers, allows one concurrent run, and has a
three-hour timeout. These are deliberately small initial settings and should be
revisited using the first backfill's CloudWatch duration and worker-utilization
metrics.

## Manual run

Start an independent reconciliation with:

```shell
make glue-start
```

The command returns a Glue job-run ID. Inspect it with:

```shell
aws glue get-job-run \
  --region us-east-1 \
  --job-name zavant-analytical-projection-prod \
  --run-id <job-run-id>
```

The first run creates 25 revision-aware analytical Iceberg tables,
`current_game_revisions`, and 25 ordinary Athena views named `current_<table>`
in `zavant_analytical_prod`. The views expose explicit business columns and
resolve each history table through the current-revision mapping; dbt consumes
these views without handling revision or projection identifiers. The `games`
history table is written last and acts as the revision-completion marker.

The job creates or replaces the current views through the `primary` Athena
workgroup, then updates the current-revision mapping. View DDL writes query
metadata beneath `lake/analytical/athena-results/projection-views/`. A
successful rerun with no newly landed revisions should report zero projected
revisions while still reconciling the current mapping; unchanged view
definitions are reused.

Steady-state runs avoid repeated catalog work. Glue inventories the catalog
once, creates only missing Iceberg tables, and still validates every table
against its exact Python contract. One S3 listing classifies both immutable
revision metadata and current pointers. Pointer objects older than their
successfully reconciled Iceberg mapping are not downloaded; new or changed
pointers and a five-minute overlap are validated with bounded concurrent reads.
The overlap protects updates near an S3 timestamp/reconciliation boundary. The full immutable
revision inventory is still anti-joined to `games`, so this optimization does
not make correctness depend on the preceding acquisition run.

Current-view definitions are content-fingerprinted beneath
`lake/analytical/control/current-views.json`. The 25 Athena DDL statements run
only when a definition changes, the marker is absent, or a catalog view is
missing. Iceberg MERGEs include `season` in their match predicate to enable
partition pruning. Timed phase messages in the Glue output log expose catalog,
schema, S3 inventory, pointer resolution, completion scan, table merge, view,
and current-mapping costs independently.

Each analytical generation supports one projection contract. Before deploying
a new contract release, delete both the Glue Catalog tables and analytical
Iceberg objects with the reviewed scripts in `scripts/adhoc`, then run Glue to
rebuild every immutable raw revision. A contract mismatch fails rather than
mixing releases in the same tables.
