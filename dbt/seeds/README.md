# Annual wOBA weights

`woba_weights.csv` is a small, version-controlled reference dataset, not a raw
acquisition output. `dbt seed` (or `dbt build` including the seed) loads it into
the selected target's Athena database. It has one row per MLB season.

The six coefficients were transcribed on **2026-09-27** from the public
[FanGraphs seasonal constants table](https://www.fangraphs.com/tools/guts?sort=7%2Ca&type=cn),
using its displayed three-decimal precision. `walk_weight` corresponds to `wBB`,
`hit_by_pitch_weight` to `wHBP`, and the remaining four to `w1B` through `wHR`.
The reference covers the lake's 2015–2026 regular seasons. The retrieved 2026
values are an **in-season snapshot** and are explicitly provisional. Retrieval
date records when we consulted the source, not when FanGraphs calculated it.

The [standard wOBA formula](https://library.fangraphs.com/offense/woba/) is:

```text
(wBB × unintentional BB + wHBP × HBP + w1B × 1B + w2B × 2B + w3B × 3B + wHR × HR)
───────────────────────────────────────────────────────────────────────────────
                       AB + unintentional BB + HBP + SF
```

The PA fact attaches the matching season's contribution to each row. MetricFlow
sums those contributions and opportunities before division, including for
multi-season totals. It does not average season rates. These are observed
outcomes: this reference does not change Savant-supplied xwOBA or xwOBAcon.
Rounded coefficients and source attribution differences mean exact parity with
every Savant player-page aggregate is not guaranteed.

## Maintaining the reference

1. Review the public table periodically during the active season and after the
   season ends. Do not treat the provisional row as a live feed.
2. Update coefficients, provenance, and `retrieved_on` together; mark the season
   non-provisional after verifying the end-of-season values. Add a verified row
   before loading games from a new season. Never copy the preceding season's
   weights as a fallback.
3. Load and test the seed, then rebuild the PA fact incrementally:

   ```shell
   # From the repository root, using your intended target.
   .venv/bin/dbt build --project-dir dbt --target prod --select woba_weights
   .venv/bin/dbt build --project-dir dbt --target prod --select fct_plate_appearances
   ```

The fact stores a hash of the six coefficients, season, and provisional flag.
When any of those change, the next PA build replaces games for that season even
if acquisition revisions are unchanged. Changing only the retrieval date does
not cause a rebuild. Ordinary model-logic changes can still require a full
refresh. A runtime pre-hook rejects missing, duplicate, or invalid weights
before writing the fact, including when only the fact is selected.

Dagster discovers the seed through `ref('woba_weights')`: it appears as
`analytics/woba_weights` in `dbt_reference`, joins the independent branch, and
must finish before the combined-source PA branch. Editing a seed does **not**
create a new publication event in the source-monitoring sensor; use the manual
commands above to apply a same-day reference correction. Coordinate manual
writes with Dagster so they do not overlap.

## First deployment

The existing PA Iceberg table needs four new columns. Its schema-change policy
is `ignore`, so the first rollout needs a **full refresh of this fact**, not just
an incremental run. Pause the Dagster sensor and let active/queued runs finish
before deploying; stop the local server while changing code. Then run:

```shell
.venv/bin/dbt build --project-dir dbt --target prod --select woba_weights
.venv/bin/dbt build --project-dir dbt --target prod --full-refresh --select fct_plate_appearances
```

These commands assume existing staging/intermediate models are already current;
use `--select +fct_plate_appearances` in the second command if they also need
rebuilding. Repeat for `--target dev` if that environment is used. Reload the
Dagster code location before re-enabling the sensor, and publish the updated
semantic definitions to Hex once Context Sync is available. This implementation
does not itself run seeds, rebuild warehouse tables, or publish to Hex.
