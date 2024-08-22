import boto3
import pyathena
import subprocess
import pandas as pd

database = "zavant_dev"
temp_bucket = "s3://aws-athena-query-results-995283862400-us-east-1-yfge1n3q/"
dbt_bucket = "s3://zavant-dbt-dev/"

athena = boto3.client("athena")
conn = pyathena.connect(
    region_name="us-east-1", s3_staging_dir="s3://zavant-dbt-dev/tables/zavant_dev"
)


def get_tables():
    tbls_query = f"select table_name from information_schema.tables where table_schema = '{database}';"
    tbls = pd.read_sql(tbls_query, conn).table_name
    return tbls


def drop_table(table):
    response = athena.start_query_execution(
        QueryString=f"drop table if exists {table};",
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": temp_bucket},
    )
    return response["QueryExecutionId"]


def clear_bucket(bucket):
    command = f"aws s3 rm {bucket} --recursive"
    subprocess.run(command, shell=True, check=True)


if __name__ == "__main__":

    for tbl in get_tables():
        drop_table(tbl)

    for bucket in (temp_bucket, dbt_bucket):
        clear_bucket(bucket)
