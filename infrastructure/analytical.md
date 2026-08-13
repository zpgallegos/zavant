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

The first run creates 25 analytical Iceberg tables plus
`current_game_revisions` in `zavant_analytical_prod`. The `games` table is
written last and acts as the revision-completion marker. A successful rerun
with no newly landed revisions should report zero projected revisions while
still reconciling the current-revision mapping.

Each analytical generation supports one projection contract. Before deploying
a new contract release, delete both the Glue Catalog tables and analytical
Iceberg objects with the reviewed scripts in `scripts/adhoc`, then run Glue to
rebuild every immutable raw revision. A contract mismatch fails rather than
mixing releases in the same tables.
