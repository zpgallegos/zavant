# Dagster: external lake publications → selective dbt builds

EventBridge and Step Functions still own acquisition and Glue projection.
Dagster **only reads their completion evidence** and runs dbt. There is no
Dagster acquisition job, Glue invocation, or daily schedule to compete with AWS.

```text
EventBridge → Step Functions → Stats API / Savant → Glue
                                                   │ publishes externally
                              history tables + current revision mappings
                                                   │
                                        current-state Athena views
                                                   │ actual source()/ref() edges
                                        analytics/<dbt model>
```

## The graph and the execution boundary

[`assets/athena.py`](../src/zavant/orchestration/assets/athena.py) defines
57 external `AssetSpec`s: two raw S3 datasets, 29 Athena history/control tables,
and 26 current-state views. Their inventory comes from **producer contracts**,
including relations dbt does not consume. They have no materialization function.

- Each history/control table depends on its own acquisition, not both sources.
- Each current view depends on its history table and source-specific revision
  mapping. Its SQL definition is metadata, not something recreated by Dagster.
- [`assets/dbt.py`](../src/zavant/orchestration/assets/dbt.py) translates dbt
  `source()` references to those physical keys and preserves `ref()` edges.
  Only the 38 `analytics/...` dbt models are executable Dagster assets.
- dbt YAML semantic models/metrics are definitions, not additional buildable
  tables. The SQL time-spine model remains an ordinary dbt asset.

Asset keys and useful UI groups remain unchanged. Existing event history can
therefore remain associated with the same datasets despite the ownership change.
The job is now `build_dbt`, not `daily_pipeline`.

## How readiness works

[`sources.py`](../src/zavant/orchestration/sources.py) describes two publication
families, `stats_api` and `savant`. A processing cycle is a local calendar day,
defaulting to `America/Los_Angeles`; override `ZAVANT_DAGSTER_CYCLE_TIMEZONE` if
needed. This matches this project's morning daily acquisitions. It is **not**
a general business-date solution for loads spanning midnight.

[`publication.py`](../src/zavant/orchestration/publication.py) requires:

1. The latest daily acquisition attempt started in this cycle and completed
   successfully. A newer incomplete/failed attempt blocks that source; an older
   success is not a substitute. The read-only
   [`acquisition` resource](../src/zavant/orchestration/resources/acquisition.py)
   reads existing S3 manifests, including Stats API's legacy `runs/daily/` path.
2. Athena's revision registry is nonempty, has unique entity keys, and has one
   projection identity with the expected contract version. Every mapping must
   reference its completion marker: `games` for Stats, `statcast_dates` for
   Savant, at the same source revision and contract version.
3. The reconciliation timestamp is at or after that acquisition completed.
   Old history rows alone cannot establish today's readiness. A successful
   acquisition with unchanged content is allowed.

The terminal marker is evidence of the **producer's ordered-write contract**,
not an exhaustive audit of every history table. The current views share their
source family's publication boundary. All that family's relation checks thus
use the same validation result, while retaining separate lineage and check
entries in the UI. There are no invented table-level completion timestamps.

For models combining both sources, both registries must also identify the same
Glue projection run. That avoids treating Glue's sequential registry updates,
or a partially failed publication, as a complete combined publication.

## Sensors trigger; checks guard

[`sensors/athena.py`](../src/zavant/orchestration/sensors/athena.py) contains one
stopped-by-default sensor, `monitor_athena_publications`. While enabled with the
daemon running, it polls no more frequently than every five minutes. A failed
source query leaves that source unready but does not hide the other source.

For a newly ready source it records **external materialization events** on that
source's raw/table/view assets. These mean “Dagster observed an external
publication,” not “Dagster ran Glue.” Metadata includes acquisition run ID,
manifest URI, projection ID, processing cycle, and validation query ID.

The same sensor requests eligible dbt subsets. Its cursor deduplicates observed
publication events. Run keys and stored run outcomes separately deduplicate
build attempts and establish which upstream dbt work actually succeeded.
Committing a cursor is not evidence that dbt built successfully.

[`jobs.py`](../src/zavant/orchestration/jobs.py) derives branches from each
model's **transitive dbt source dependencies**, not a hand-written model list:

| Branch | Current models | Eligibility |
| --- | ---: | --- |
| Stats API | 33 | Stats acquisition and projection ready |
| Savant | 2 | Savant acquisition and projection ready |
| Combined | 2 | Both sources published by the same Glue run, and their prerequisite dbt branches succeeded for these publications |
| Independent | 1 | Time spine; once per cycle when either source first becomes ready |

For example, `fct_pitches` can build without Savant. `fct_batted_balls` and
`fct_plate_appearances` wait for both sources and their dbt parents. A Savant-only
update does not automatically rerun Stats-only models. New projection IDs count
as new publications, even if the underlying baseball content is unchanged.

Inside a requested branch, Dagster/dbt use the actual model graph and selected
tests. This is **source-family branch automation**, not per-table independent
publication or one run per model. A failing model can hold up other models in
its branch. If the producer later publishes individual tables independently,
the readiness contracts and branch planner should become correspondingly finer.

[`checks/athena.py`](../src/zavant/orchestration/checks/athena.py) supplies blocking
`published_after_acquisition` checks. They run with the requested subset, query
each selected family once, and fail before dbt if inputs are no longer ready.
The dbt function also performs preflight so direct UI materializations that omit
external checks cannot bypass readiness. Neither checks nor `deps` by themselves
start builds; the sensor does that.

The sensor also enforces successful prerequisite dbt branches. A manual UI
subset does not get that planning step: include the needed upstream dbt models
or ensure they have already built against the current publications.

Queued runs carry expected projection IDs and cycle in their config. Preflight
rejects changed publications or an expired cycle rather than silently consuming
different inputs. This is **not snapshot isolation**: an external Glue run can
still change live views during dbt execution. Strict reproducibility would need
pinned input snapshots or producer/consumer coordination, which is not provided.

## Run it locally

1. Install the project's development/orchestration dependencies and configure
   `.env` from `.env.example`. dbt defaults to `~/.dbt/profiles.yml` and the `dev`
   target; `DBT_PROFILES_DIR` and `DBT_TARGET` override those choices. Confirm the
   target before launching anything: dbt materializations write real Athena data.
2. Run `make dagster-dev`. The existing AWS daily workflow should remain enabled.
   In development Dagster refreshes the dbt manifest. The separate-service path
   uses `make dagster-prepare` first; see the deployment runbook below.
3. Confirm only dbt assets offer materialization, and inspect lineage such as
   `pitches → current_pitches → analytics/stg_pitches` plus the revision-mapping
   edge into `current_pitches`.
4. Preview `monitor_athena_publications` under Automation. Preview performs S3
   reads and Athena SELECT queries (normal query charges), but does not invoke
   acquisitions, start Glue, or execute dbt. Inspect requested model selections,
   run configuration, and computed cursor. UI preview/commit is not a substitute
   for a real daemon tick; a committed cursor alone does not prove asset events
   were persisted. Reset the sensor cursor before enabling it if preview
   committed a publication without recording its events.
5. When ready to permit real dbt writes, enable this sensor. Monitor actual ticks,
   `build_dbt` runs, relation checks, and asset Events. The optional
   `notify_run_failure` sensor requires an SNS topic and separate activation.

Both instance templates serialize runs (`max_concurrent_runs: 1`) to avoid
overlapping dbt writes. Initialization **never overwrites an existing**
`.local/dagster/dagster.yaml`. Stop Dagster and merge the `run_coordinator` block
from [`dagster.yaml`](../infrastructure/dagster/dagster.yaml) into an older home,
or test with a new `DAGSTER_HOME`. Do not delete the old SQLite history/cursors.
The queue needs a running daemon and does not coordinate separate instances.

## Recovery and limits

- A failed/canceled build is not automatically retried on each sensor tick.
  Inspect it and reexecute the run in the UI, preserving its identity tags and
  configuration. Successful reexecution unblocks dependent branches. A genuinely
  new publication gets a new build identity. Clearing the sensor cursor alone
  will not reset run-key deduplication.
- If the publication changed while queued, an old run should fail preflight;
  let the next tick plan current-publication work instead. Investigate stuck
  active runs before marking them finished; they otherwise hold new submissions.
- This monitors current-day publications, not historical replay. Backfill-only
  Glue runs without a current daily acquisition do not trigger builds, and
  yesterday's unfinished cycle is not automatically caught up after midnight.
- This is not content-diff selection. The mapping registry is reconciled by
  Glue as a unit. The sensor cannot infer independent completion of each table
  or pinpoint which models' rows changed from a projection ID alone.
- Both evidence queries scan registry/marker data; polling and dbt have Athena
  costs. Manifest reads list the existing daily prefixes. This learning-scale
  approach would need more efficient publication indexes at larger scale.
- Sensor read errors are visible in tick logs and skip reasons; the optional
  failure sensor covers failed **runs**, not missing publications or sensor
  errors. Missed-cycle/freshness alerts are not implemented.

## Code map and retained infrastructure

`definitions.py` assembles external assets, dbt assets, checks, the one job, two
sensors, and four resources. The remaining resources are Athena queries, S3
acquisition evidence, dbt CLI, and optional SNS notifications. Lambda invocation,
Glue-job resources, executable acquisition/projection assets, old observation
sensors, and the full-pipeline daily schedule have been removed.

Persistent local SQLite storage, the three-service setup, and the optional EC2
deployment remain useful. EC2 IAM now permits source reads and dbt writes, **not
Lambda invocation or Glue job execution**. See the
[deployment and recovery runbook](../infrastructure/dagster/README.md).

Tests cover source failure/delay, stale and partial publications, manifest
pagination, branch ordering/deduplication/reexecution, preserved lineage, actual
Dagster subset execution with mocked AWS/dbt, blocking checks, and manual-build
preflight. No test proves live AWS IAM or warehouse behavior; those require an
explicitly authorized live acceptance run.
