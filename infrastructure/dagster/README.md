# Deploying the Dagster publication monitor

This optional single-host **learning deployment** monitors externally published
Athena relations and runs dbt. EventBridge/Step Functions continue running the
acquisitions and Glue. Dagster has no Lambda-invocation or Glue-job permissions.
See the [workflow guide](../../docs/dagster.md) for readiness and branch behavior.

Local tests do not prove a live EC2 deployment works. Creating this stack incurs
charges; no deployment or automation activation is implicit in these files.

## Three services, one persistent instance

| Service | Local command | Responsibility |
| --- | --- | --- |
| Code server | `make dagster-code-server` | Loads definitions and hosts run workers |
| Webserver | `make dagster-webserver` | UI and run submission |
| Daemon | `make dagster-daemon` | Sensor evaluation and queued run launch |

[`workspace.yaml`](workspace.yaml) points to `127.0.0.1:4000`; the UI listens on
`127.0.0.1:3000`. Loopback works both locally and on EC2 because these services
share one host. All three must share the same `DAGSTER_HOME` and environment.

Before loading definitions, [`prepare.py`](../../src/zavant/orchestration/prepare.py)
runs `dbt deps` and `dbt parse --no-partial-parse`. This downloads dependencies
and creates the manifest, but does not execute `dbt build` or query Athena.
Preparation uses the selected profile directory and `DBT_TARGET`; errors stop
startup. Re-prepare after changing dbt code/packages/target, with runs drained
and the code server stopped.

To test separate services locally, stop `dagster dev` first. In each terminal:

```sh
export DAGSTER_HOME="$PWD/.local/dagster-services"
export DBT_TARGET=dev
```

Initialize and prepare once, then start each service in its own terminal:

```sh
make dagster-service-init
make dagster-prepare
```

Both [`dagster.yaml`](dagster.yaml) and
[`dagster-service.yaml`](dagster-service.yaml) queue runs with
`max_concurrent_runs: 1`. The latter also configures run monitoring and disables
automatic retries/resume. A new home does not inherit history or enabled sensor
state from `.local/dagster`. Initialization never overwrites an existing config;
stop services and review/merge template changes when reusing an older home.

## Optional EC2 host

[`../dagster-stack.yaml`](../dagster-stack.yaml) defines:

- Amazon Linux 2023, default `t3.medium`, instance-role credentials, IMDSv2,
  and no inbound security-group rules.
- An encrypted 20 GiB EBS data volume mounted at `/var/lib/zavant` for SQLite
  history, cursors, and compute logs. It is retained on deletion/replacement;
  the replaceable root disk holds code and the virtual environment.
- A dedicated Athena workgroup with encrypted S3 results and a 10 GiB per-query
  scan limit. This is **not** a monthly spending cap.
- Read access to acquisition manifests and the analytical lake/catalog; scoped
  dbt S3/catalog writes and query-result access. No producer execution permissions.
- SNS notifications and an EC2 status-check alarm, with optional confirmed email.

[`install.sh`](install.sh) identifies the exact EBS volume ID, formats it only
when empty, installs dependencies, and enables systemd services. A missing
state disk prevents startup. [`zavant-dagster-prepare.service`](zavant-dagster-prepare.service)
prepares the manifest once per boot. [`zavant-dagster@.service`](zavant-dagster@.service)
runs `code`, `webserver`, and `daemon` as a non-root user and restarts exited
services. Code/UI health checks precede the CloudFormation success signal.

All services read `/etc/zavant/dagster.env`, including:

```text
DAGSTER_HOME=/var/lib/zavant/dagster
DBT_PROFILES_DIR=/opt/zavant/app/infrastructure/dagster
DBT_TARGET=prod
```

[`profiles.yml`](profiles.yml) has no credentials and uses the EC2 role. The
deployment targets existing `zavant_analytical_prod` sources and `zavant_dbt_prod`
outputs. The repo's dbt source definitions are prod-specific; an environment
variable alone does not retarget them to a different lake.

### Review before launching

1. Choose an existing VPC/public subnet and matching AZ. An Internet Gateway
   route is needed for downloads/AWS APIs. No VPC/NAT gateway is created. Public
   IPv4 provides outbound connectivity, not a public UI, and has its own charges.
2. Confirm account, region, bucket, and data prefix from the existing stacks.
3. Set `DbtDataPrefix` to the existing production dbt S3 directory, relative to
   that bucket. Incremental tables may already reference it. Other buckets,
   customer-managed KMS keys, or Lake Formation restrictions require additional
   reviewed permissions; the supplied policy does not cover them.
4. Keep Dagster sensors stopped until validation is complete. Keep the external
   AWS workflow enabled. Only one Dagster instance should automate these dbt
   outputs; the run queue does not coordinate multiple instances.

### Build and deploy explicitly

```sh
make dagster-package
make dagster-infra-validate
```

Packaging is local; validation sends the template to AWS but does not create a
stack. The release allowlist excludes `.env`, local profiles, `.local`, `.venv`,
logs, and generated dbt artifacts. It includes uncommitted source changes, so
review the working tree before uploading.

After setting the reviewed `AWS_REGION`, `DATA_BUCKET`, `DATA_PREFIX`,
`DBT_DATA_PREFIX`, `VPC_ID`, `SUBNET_ID`, and `AVAILABILITY_ZONE`:

```sh
make aws-check-account AWS_REGION="$AWS_REGION"
release_sha="$(shasum -a 256 build/zavant-dagster.tar.gz | awk '{print $1}')"
release_key="deployments/dagster/$release_sha.tar.gz"
aws s3 cp build/zavant-dagster.tar.gz "s3://$DATA_BUCKET/$release_key" --region "$AWS_REGION"
aws cloudformation deploy --region "$AWS_REGION" \
  --stack-name zavant-dagster-prod \
  --template-file infrastructure/dagster-stack.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    VpcId="$VPC_ID" SubnetId="$SUBNET_ID" AvailabilityZone="$AVAILABILITY_ZONE" \
    DataBucketName="$DATA_BUCKET" DataPrefix="$DATA_PREFIX" \
    DbtDataPrefix="$DBT_DATA_PREFIX" \
    ReleaseKey="$release_key" ReleaseSha256="$release_sha"
```

`AlertEmail=you@example.com` is optional and needs recipient confirmation. The
operator needs normal CloudFormation/EC2/IAM/pass-role/SSM/Athena/SNS/S3 deployment
permissions, not merely the restricted host role. The checksum pins the archive;
this is not a fully locked/prebuilt machine image.

This is an initial-install template, not a rolling-release controller. Changing
`ReleaseKey` does not rerun cloud-init on an existing host. For updates, disable
the monitor, drain runs, stop services, back up state, deliberately install the
reviewed release/venv, prepare, and restart. Host replacement with retained state
requires a planned stopped-host detach/reattach or snapshot restore in the same
AZ; do not assume a still-attached state disk migrates automatically.

### Access and inspection

Run the stack's `UiTunnelCommand` output locally using the AWS Session Manager
plugin, then open `http://localhost:3000`. The operator needs permission to start
an SSM session. There is no public UI, SSH key, reverse proxy, or application
authentication: AWS access to the tunnel is the security boundary. Do not change
the bind address/open inbound ports without adding authentication.

In an SSM shell:

```sh
sudo systemctl status zavant-dagster@code zavant-dagster@webserver zavant-dagster@daemon
sudo journalctl -u zavant-dagster-prepare -u zavant-dagster@daemon -n 100
sudo journalctl -u cloud-final -n 100
```

## Acceptance and operations

1. Confirm 57 external assets, 38 executable dbt models, `build_dbt`, no Dagster
   schedules, and two stopped sensors. Check all services and daemon heartbeats.
2. Verify the dbt target, source-read permissions, and dbt write locations.
   A healthy code server does not prove warehouse IAM works.
3. Preview `monitor_athena_publications` after an external daily publication.
   This issues Athena SELECTs and reads S3, but does not execute dbt. Verify
   the expected source-specific run selections/configuration. If a preview
   commit advanced the cursor without recording events, reset that cursor
   before the first real tick.
4. Explicitly enable the monitor when ready to authorize dbt writes. Verify
   source events, blocking checks, Stats/Savant branch runs, then the combined
   branch. EventBridge remains the producer's scheduler throughout.
5. Optionally configure and enable `notify_run_failure` before a planned bounded
   failure test. It sends job/run identifiers, not raw exception text; it neither
   retries jobs nor backfills old failures on activation. SNS may duplicate
   messages, and a publish error requires investigation.
6. While idle, restart services, then test a reboot and confirm history survives
   and preparation/services start automatically. These live acceptance steps
   are not covered by mocked tests.

An opt-in local smoke test starts all three real services with an isolated home,
checks loading/heartbeats, and confirms no runs launched:

```sh
ZAVANT_TEST_DAGSTER_SERVICES=1 PYTHONPATH=src .venv/bin/python -m unittest tests.test_dagster_services
```

The one-run queue serializes requests, including UI launches, but not external
Glue activity or writes from other Dagster instances. Publication IDs checked
before a build do not pin Athena snapshots during the build. Avoid overlapping
external reruns when consistent inputs matter.

Automatic retries/resume are disabled. Inspect Athena/dbt state before retrying
after a lost worker: queries may still be running. The default launcher does
not recover lost workers after a host crash. A stale STARTED run can block the
queue until you reconcile its state. Preserve original config/tags when
reexecuting a failed sensor-requested run so downstream readiness recognizes it.

The EC2 alarm detects host status failures, not a stuck daemon, disk exhaustion,
sensor errors, or missing source publications. Inspect ticks, heartbeats, disk,
and logs. External freshness/heartbeat alerts and automated backups are future
hardening, not implemented here.

## Backups and recovery

SQLite is intentionally single-host. A retained EBS volume is **not a backup**.
Disable sensors, drain work, stop all services, then snapshot the volume or back
up the complete stopped `DAGSTER_HOME`. Independently copying live SQLite files
does not ensure consistency. Test restores in isolation with automation off:
restored state includes enabled sensors and may launch real work on daemon start.

Restore into the same AZ, verify mount/ownership/config, and use matching pinned
application/dependency versions. Reconcile interrupted queries and Dagster's
active/queued runs before restarting the daemon.

Deleting the stack terminates the host but retains the data volume and its
charges. Record `DataVolumeId`; retained volumes, snapshots, and release objects
require deliberate cleanup when no longer needed.
