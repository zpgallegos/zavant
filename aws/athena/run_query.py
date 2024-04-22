import boto3

from sqlparse import parse, sql


QUERY_FILE = "create-standard-batting.sql"
DB = "zavant"
OUTPUT_BUCKET = "zavant-intermediate"
UNSAVED_PREFIX = f"s3://{OUTPUT_BUCKET}/Unsaved"

athena = boto3.client("athena")
s3 = boto3.client("s3")


def delete_prefix(prefix: str):
    """
    delete all objects with the given prefix in the output bucket
    used to delete the underlying data of a table when it is dropped

    :param prefix: the prefix to delete
        this will be the same as the table name by convention
    :return: None
    """
    paginator = s3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate(Bucket=OUTPUT_BUCKET, Prefix=prefix)
    for page in page_iter:
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            s3.delete_object(Bucket=OUTPUT_BUCKET, Key=key)
            print(f"Deleted {key}")


def drop_table(statement: sql.Statement):
    """
    drop the table specified in the DROP statement

    :param statement: the DROP statement
    :return: None
    """
    assert statement.get_type() == "DROP"
    for token in statement:
        if isinstance(token, sql.Identifier):
            dropped_table = token.get_real_name()
            break
    else:
        raise ValueError("DROP statement does not contain table name")

    delete_prefix(dropped_table)
    print(f"Deleted {dropped_table}")


def run_query(statement: sql.Statement):
    """
    run the given statement in Athena

    :param statement: the statement to run
    :return: the QueryExecutionId of the query
    """
    if statement.get_type() == "DROP":
        drop_table(statement)

    response = athena.start_query_execution(
        QueryString=str(statement),
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": UNSAVED_PREFIX},
    )
    return response["QueryExecutionId"]


if __name__ == "__main__":
    with open(QUERY_FILE, "r") as f:
        query = f.read()

    statements = parse(query)
    for statement in statements:
        run_query(athena, statement)
