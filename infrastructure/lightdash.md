# Lightdash integration infrastructure

The `zavant-lightdash-prod` stack is the warehouse boundary for Lightdash
Cloud. It does not run the Lightdash application. It owns:

- A dedicated Athena workgroup named `zavant-lightdash-prod`.
- A private, encrypted S3 bucket for Athena query results.
- A dedicated IAM user whose inline policy can query only the production dbt
  database and read only the production dbt table prefix.

Lightdash query results expire after seven days. The workgroup enforces its
result location, publishes CloudWatch metrics, and rejects any individual query
that scans more than 10 GiB. Both controls are configurable in `.env`.

The warehouse identity cannot read raw MLB responses, write dbt tables, modify
the Glue Data Catalog, or use another Athena workgroup. The query-results bucket
is retained if the stack is deleted, but its lifecycle rule continues to remove
objects.

## Validate and deploy

The deploy target retrieves the existing data bucket and prefix from the
acquisition stack:

```shell
make lightdash-infra-validate
make lightdash-infra-deploy
make lightdash-infra-outputs
```

The production defaults are:

```text
database: zavant_dbt_prod
query result retention: 7 days
per-query scan limit: 10 GiB
```

The database must exist before Lightdash can query it. Build and test the full
production dbt project before connecting Lightdash:

```shell
make dbt-prod-build
```

## Create the Lightdash credential

CloudFormation deliberately does not create an IAM access key. Secret access
keys are returned only when created, and putting one in the template would
expose it through CloudFormation state and outputs.

After deploying, retrieve the user name and create its first access key:

```shell
user_name="$(aws cloudformation describe-stacks \
  --region us-east-1 \
  --stack-name zavant-lightdash-prod \
  --query 'Stacks[0].Outputs[?OutputKey==`LightdashWarehouseUserName`].OutputValue | [0]' \
  --output text)"

aws iam create-access-key --user-name "$user_name"
```

AWS displays the secret access key only in that response. Enter it directly
into the Lightdash warehouse connection; do not add it to `.env`, source
control, CloudFormation parameters, or shell history. Keep at most one active
key except during an intentional rotation.

## Configure Lightdash

Choose Amazon Athena as the warehouse and use the stack outputs:

| Lightdash setting | Value |
|---|---|
| Region | `us-east-1` |
| Database | `LightdashDatabaseName` |
| S3 staging directory | `LightdashQueryResultsS3Uri` |
| Workgroup | `LightdashAthenaWorkGroupName` |
| AWS access key ID | Access-key creation response |
| AWS secret access key | Access-key creation response |

Leave the optional S3 data directory empty. Lightdash queries dbt relations but
does not materialize this project's dbt models. The dbt project directory in
the repository is `/dbt`. Set the Lightdash dbt selector to `tag:lightdash` so
only intentional presentation marts become Explores; staging and intermediate
models remain implementation details. This selector controls what Lightdash
exposes; it is deliberately not used to truncate the production dbt build,
whose complete test graph references models outside the presentation mart's
direct dependency graph.

Lightdash reads MetricFlow resources from the compiled dbt manifest and
translates the supported metrics into its own Athena SQL. It does not use the
MetricFlow Athena runtime, so MetricFlow's missing Athena adapter does not
affect Lightdash query execution.
