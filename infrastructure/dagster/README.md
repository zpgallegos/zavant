# Running the Dagster publication monitor locally

This portfolio setup monitors externally published Athena relations and runs
dbt on your machine. EventBridge/Step Functions continue running acquisitions
and Glue independently. Dagster does not invoke those producers. See the
[workflow guide](../../docs/dagster.md) for readiness and branch behavior.

The EC2 template and its packaging/bootstrap files have been removed. No
dedicated Dagster AWS stack is needed. This removes dedicated hosting costs,
not Athena/S3 query and storage charges.

## Start and inspect

From the repository root:

```sh
make dagster-dev
```

Open `http://localhost:3000`. This development command starts the UI, code
server, and daemon together. It reads the Make configuration, defaults to
`~/.dbt/profiles.yml` with `DBT_TARGET=dev`, and refreshes the dbt manifest.
Confirm the actual destination in your profile: local orchestration still
builds real tables in Athena. Local AWS credentials need source/catalog reads,
Athena query access, and writes to the configured dbt/output locations.

State lives under `.local/dagster` by default. Both instance templates queue
runs with `max_concurrent_runs: 1`, but initialization never overwrites an
existing `dagster.yaml`. For an older home, stop Dagster and merge the queue
configuration from [the local template](dagster.yaml). Keep its SQLite files.

The monitor is stopped by default **only until its activation state is saved**.
A previously enabled `monitor_athena_publications` resumes when the daemon
starts. Starting the UI is therefore not necessarily observation-only.

For a separate, initially stopped instance:

```sh
make dagster-dev DAGSTER_HOME="$PWD/.local/dagster-inspect"
```

Use this for inspection, not a second enabled monitor for the same dbt outputs.
It has independent history, cursors, and run deduplication.

## What enabling the monitor does

1. Every eligible tick reads today's acquisition manifests and queries Athena's
   revision registries and completion markers. A source can be discovered even
   if its publication completed before Dagster started.
2. Newly ready sources get external materialization events on their raw and
   Athena assets. These record observed AWS publication, not Dagster execution
   of acquisitions or Glue.
3. With both sources ready and no previous attempts or active runs, the sensor
   requests three `build_dbt` subsets: the independent time spine (1 model),
   Savant-only (2), and Stats-only (33). The queue serializes their execution.
4. Each source-dependent run validates its inputs again before building dbt.
   A later sensor tick requests the combined branch (2 models) after its
   prerequisite dbt branches succeed for the same publication identities.
5. Unchanged publications are not rebuilt every tick. Failed/canceled attempts
   require investigation and deliberate reexecution; they are not retried in a
   five-minute loop.

Use Automation to inspect sensor ticks, Runs for `build_dbt` selections/logs,
and asset Events/Checks for publication metadata and readiness results. Preview
issues S3 reads and Athena SELECTs but does not execute dbt. Do not commit a
preview merely to inspect it: committing can change cursor/run state.

The optional `notify_run_failure` sensor needs an existing SNS topic, publish
permissions, and separate activation. Leave it stopped if you do not need
notifications; no topic is created by the local setup.

## Stop, restart, and recover

For a deliberate shutdown, disable the monitor, let queued/running work finish,
then press Ctrl-C in the `make dagster-dev` terminal. Its saved state and history
survive. Re-enable the monitor when ready after restarting.

While the process is stopped or the laptop sleeps, no sensors evaluate and no
new dbt work is launched by that local instance. The external AWS daily
workflow is unaffected. The monitor handles the current local calendar day,
not automatic replay of missed days. Run Dagster on the publication day if you
want that cycle processed automatically.

This is an explicit portfolio availability tradeoff, not a continuously
available production control plane. Production operation would normally keep
the daemon running with durable state, operational monitoring, and tested
backups.

Avoid stopping mid-build: Athena queries can outlive their local worker.
Inspect warehouse and Dagster state before retrying an interrupted run. Stale
active runs block new requests. Preserve the original config/tags when
reexecuting a sensor-requested run so its dependent branches recognize success.

SQLite is single-host metadata storage, not a backup. Drain work and stop
Dagster before backing up the entire `DAGSTER_HOME`; copying individual live
SQLite files does not ensure consistency. Restored state includes enabled
sensors, so inspect it before restarting automation.

The queue does not coordinate other Dagster instances or external Glue writes.
Publication checks do not pin Athena snapshots during a build. Avoid external
reruns while dbt is consuming a publication when consistent inputs matter.

## Optional: inspect the three services separately

Stop `dagster dev` first. In each terminal, use the same environment and home:

```sh
export DAGSTER_HOME="$PWD/.local/dagster-services"
export DBT_TARGET=dev
```

Initialize and prepare once:

```sh
make dagster-service-init
make dagster-prepare
```

Preparation runs `dbt deps` and `dbt parse --no-partial-parse`, not a build.
Re-prepare after changing dbt code/packages/target, with work drained and the
code server stopped. Then start each service in its own terminal:

| Service | Command | Responsibility |
| --- | --- | --- |
| Code server | `make dagster-code-server` | Loads definitions and hosts run workers |
| Webserver | `make dagster-webserver` | UI and run submission |
| Daemon | `make dagster-daemon` | Sensor evaluation and queued run launch |

[The workspace](workspace.yaml) points to `127.0.0.1:4000`; the UI listens on
`127.0.0.1:3000`. [The service template](dagster-service.yaml) also enables run
monitoring and disables automatic retries/resume. A new home does not inherit
history or sensor activation from `.local/dagster`.

An opt-in smoke test starts these services with isolated state and no enabled
sensors or dbt runs:

```sh
ZAVANT_TEST_DAGSTER_SERVICES=1 PYTHONPATH=src .venv/bin/python -m unittest tests.test_dagster_services
```
