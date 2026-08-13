# Zavant dbt project

This directory is the transformation boundary between the revision-aware
Iceberg tables in the Glue Data Catalog and the semantic and presentation
layers that will consume conformed analytical models.

The project is intentionally empty. Its first implementation slice will define
the Athena connection boundary, register the Iceberg sources, and build
current-revision staging models after the existing v1 dbt models have been
audited. Connection credentials and developer-specific targets belong in a
local dbt profile and must not be committed here.

The project pins dbt Core and the first-party Athena adapter in the repository's
`dev` dependency group. From the `v2` directory, run `make bootstrap` and then
`make dbt-debug`. If you prefer to invoke dbt while working in this directory,
activate `../.venv` first so the command and its adapter come from the same
environment.
