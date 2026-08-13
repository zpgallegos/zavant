# Zavant dbt project

This directory is the SQL transformation boundary between the revision-aware
Iceberg tables in the Glue Data Catalog and the semantic and presentation
layers. The current staging layer exposes every analytical dataset at its
contracted source grain while selecting only the current revision of each game.
Models above staging can therefore operate without carrying raw revision
selection logic.

The project currently includes:

- Sources for the 25 projected baseball datasets plus the projection registry
  and current-revision spine.
- One documented staging model for every source table.
- Grain and required-key tests on each staging model.
- Singular tests for the persisted relationships among revisions, games,
  plays, events, pitches, batted balls, runner movements, fielding credits, and
  boxscore player statistics.
- A freshness check on the current-revision spine, filtered to the configured
  projection contract.

The Python projection contract and
`vars.current_projection_contract_version` in `dbt_project.yml` must advance
together. The repository's Python tests enforce that configuration invariant;
an Athena-backed staging build verifies that the selected contract is present
and relationally complete in the warehouse.

## Local development

Connection credentials and developer-specific targets belong in
`~/.dbt/profiles.yml` and must not be committed here. From the repository root,
install the Python environment and dbt packages, then validate the development
connection:

```shell
make bootstrap
make dbt-debug
```

The standard local quality loop parses the dbt project without querying Athena
and lints SQL using the Athena dialect:

```shell
make check
```

The warehouse-backed checks remain explicit because they execute Athena queries
and materialize development relations:

```shell
make dbt-staging-build
make dbt-source-freshness
```

Run `make dbt-deps` after changing `packages.yml`.
