# Zavant dbt project

This directory is the SQL transformation boundary between Glue-published
current-state Athena views and the semantic and presentation layers. Glue owns
raw-revision reconciliation; dbt receives one current version of each game and
models only business grains.

The project currently includes:

- Sources for the 25 current-state baseball datasets.
- One documented staging model for every source table.
- Grain and required-key tests on each staging model.
- Singular tests for the persisted relationships among games,
  plays, events, pitches, batted balls, runner movements, fielding credits, and
  boxscore player statistics.
- A freshness check on the current games source.
- Intermediate plate-appearance and at-bat models that isolate official batter
  turns from MLB's broader allPlays stream and reconcile both grains to
  official team-game boxscore totals.
- A current-state plate-appearance fact exposing governed outcome flags,
  additive batting measures, matchup attributes, and game state before and
  after each appearance. The fact incrementally replaces complete games through
  transactional Iceberg merges so corrections can update, add, or remove rows
  without rebuilding unaffected games.

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
