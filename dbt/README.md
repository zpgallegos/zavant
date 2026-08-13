# Zavant dbt project

This directory is the SQL transformation boundary between the revision-aware
Iceberg tables in the Glue Data Catalog and the semantic and presentation
layers. The staging layer exposes every analytical dataset at its complete
revision-aware source grain. Models that need current state select it
explicitly through `stg_current_game_revisions`; revision-history models retain
all projected source revisions.

The project currently includes:

- Sources for the 25 projected baseball datasets plus the current-revision
  spine.
- One documented staging model for every source table.
- Grain and required-key tests on each staging model.
- Singular tests for the persisted relationships among revisions, games,
  plays, events, pitches, batted balls, runner movements, fielding credits, and
  boxscore player statistics.
- A freshness check on the current-revision spine.
- Intermediate plate-appearance and at-bat models that isolate official batter
  turns from MLB's broader allPlays stream and reconcile both grains to
  official team-game boxscore totals.
- An append-only, revision-grained plate-appearance fact that preserves
  superseded source revisions while exposing governed outcome flags, additive
  batting measures, matchup attributes, and game state before and after each
  appearance.

Each analytical table generation is produced by one projection contract.
Breaking projection releases rebuild the analytical tables and dependent dbt
models together; dbt does not select or include projection contract versions
in its grains.

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
