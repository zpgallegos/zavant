# Analytical projection infrastructure

The `zavant-analytics-prod` stack owns the Glue Data Catalog database, the Glue
5.0 Spark projection job, and its least-privilege execution role. It references
the existing retained acquisition bucket but does not own or replace it.

The job reads current raw-game pointers and selected revisions beneath
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
`projection_revisions` and `current_game_revisions` in
`zavant_analytical_prod`. A successful rerun with unchanged pointers should
report zero projected revisions while still reconciling the current-revision
mapping.
