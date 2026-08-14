# Hex integration infrastructure

The `zavant-hex-prod` stack creates a warehouse boundary for evaluating Hex
without changing or deleting the existing Lightdash integration. It owns:

- A dedicated Athena workgroup named `zavant-hex-prod`.
- A private, encrypted S3 bucket for ephemeral Athena query results.
- A read-only IAM role assumed by Hex with temporary credentials.

The workgroup enforces its query-result location, publishes CloudWatch metrics,
and cancels any query that scans more than 1 GiB. Query-result objects expire
after seven days. Both limits are configurable in `.env`.

The role can read only the production dbt database and production dbt table
prefix. It cannot read raw MLB responses, write dbt tables, modify the Glue
Data Catalog, or use another Athena workgroup. Prepared-statement permissions
are included because Hex Explore uses them for no-code filtering.

## Cost

Hex Team has a 14-day free trial and is currently listed at $75 per editor per
month. It includes the Threads agent, semantic-model agent, Semantic Model
Sync, scheduled runs, and extended monthly AI credits. Additional AI credits
and advanced compute can add cost if explicitly purchased.

The repository is public and the workflow uses a standard GitHub-hosted runner,
so GitHub Actions execution is free. The context workflow is small and runs
only when the dbt semantic definitions or its own workflow change.

AWS charges remain usage-based:

- Athena is $5 per TB scanned, subject to the stack's 1-GiB per-query cutoff;
  one query at that ceiling costs roughly half a cent.
- The query-results bucket incurs small ordinary S3 storage and request charges.
- IAM roles, Athena workgroups, and CloudFormation have no standing charge.

The per-query cutoff bounds one query, not total daily usage. Before granting
public users a path that can issue warehouse queries, add the separate daily
workgroup circuit breaker described in the public-serving design.

## 1. Deploy the bootstrap boundary

The first deployment creates the role with a temporary trust relationship to
this AWS account. This permits creation of the role before Hex generates its
account-specific trust policy:

```shell
make hex-infra-validate
make hex-infra-deploy
make hex-infra-outputs
```

`HexTrustConfigured` is `false` after this bootstrap deployment.

## 2. Create the Athena connection in Hex

Start the Hex Team trial, then open **Settings > Data sources > + Connection >
Athena**. Configure the connection with the stack outputs:

| Hex setting | Value |
|---|---|
| Host | `athena.us-east-1.amazonaws.com` |
| Port | `443` |
| S3 output path | `HexQueryResultsS3Uri` |
| Catalog | `AwsDataCatalog` |
| Workgroup | `HexAthenaWorkGroupName` |
| Authentication | IAM role |
| Role ARN | `HexWarehouseRoleArn` |

Save it as a draft. Hex will generate a trust policy containing an AWS
principal ARN and external ID. Add only those two non-credential values to the
ignored `.env` file:

```dotenv
ZAVANT_HEX_AWS_PRINCIPAL_ARN=arn:aws:iam::<hex-account>:role/<hex-role>
ZAVANT_HEX_AWS_EXTERNAL_ID=<hex-generated-external-id>
```

Deploy again and verify the output:

```shell
make hex-infra-deploy
make hex-infra-outputs
```

`HexTrustConfigured` must now be `true`. Return to Hex, finish connection
validation, restrict schema browsing to `zavant_dbt_prod`, and refresh the
connection schema. The trust policy intentionally applies the external-ID
condition to `sts:AssumeRole` but not `sts:TagSession`, matching Hex's IAM-role
requirements.

## 3. Create and sync the MetricFlow project

In Hex, open **Settings > Data sources > Semantic projects** and create a dbt
MetricFlow project using the Athena connection. The MetricFlow model already
includes the required warehouse table mapping:

```yaml
config:
  meta:
    hex:
      table: AwsDataCatalog.zavant_dbt_prod.fct_plate_appearances
```

Record the generated semantic-project ID in the repository-root
`hex_context.config.json` file. The ID identifies the import destination and is
not a credential. The checked-in configuration maps that project to the `dbt`
directory.

Create this GitHub repository setting:

| Kind | Name | Value |
|---|---|---|
| Actions secret | `HEX_API_TOKEN` | A Hex workspace token authorized to sync context resources |

Create the workspace token under **Hex Settings > API keys**. Do not place the
token in `.env` or any tracked file. The checked-in workflow does not expose
secrets to pull requests from forks, previews same-repository pull requests,
and publishes changes pushed to `master`. It can also be started manually from
the GitHub Actions page.

Hex's older semantic-ingestion workflow is deprecated. The repository uses the
current `hex-inc/action-context-toolkit@v2.0.0` workflow instead. Hex's generated
example currently references `@v2`, but the action repository does not publish
that moving tag, so the workflow pins the available release tag explicitly.
