"""Dagster resource for executing small validation queries in AWS Athena."""

import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

import boto3
import dagster as dg


_ACTIVE_QUERY_STATES = {"QUEUED", "RUNNING"}
_FAILED_QUERY_STATES = {"CANCELLED", "FAILED"}
_SINGLE_ROW_RESULT_PAGE_SIZE = 3

Clock = Callable[[], float]
Sleeper = Callable[[float], None]


class AthenaQueryClient(Protocol):
    """Subset of the Boto3 Athena client needed for validation queries."""

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        """Start an Athena query."""

        ...

    def get_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        """Return the current state of an Athena query."""

        ...

    def get_query_results(self, **kwargs: Any) -> dict[str, Any]:
        """Return rows produced by a successful Athena query."""

        ...


@dataclass(frozen=True)
class AthenaQueryResult:
    """Identity and single data row returned by a validation query."""

    query_execution_id: str
    row: Mapping[str, str | None]


def _query_status(response: Mapping[str, Any]) -> Mapping[str, Any]:
    query_execution = response.get("QueryExecution")
    if not isinstance(query_execution, Mapping):
        raise RuntimeError("Athena returned no query execution.")
    status = query_execution.get("Status")
    if not isinstance(status, Mapping):
        raise RuntimeError("Athena returned no query status.")
    return status


def _row_data(row: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
    data = row.get("Data")
    if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
        raise RuntimeError("Athena returned malformed row data.")
    if not all(isinstance(value, Mapping) for value in data):
        raise RuntimeError("Athena returned malformed row values.")
    return cast(Sequence[Mapping[str, Any]], data)


def _single_result_row(response: Mapping[str, Any]) -> Mapping[str, str | None]:
    result_set = response.get("ResultSet")
    if not isinstance(result_set, Mapping):
        raise RuntimeError("Athena returned no result set.")
    rows = result_set.get("Rows")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise RuntimeError("Athena returned malformed result rows.")
    if len(rows) != 2 or response.get("NextToken") is not None:
        raise RuntimeError("Athena validation query must return exactly one data row.")
    if not all(isinstance(row, Mapping) for row in rows):
        raise RuntimeError("Athena returned malformed result rows.")

    header = _row_data(cast(Mapping[str, Any], rows[0]))
    values = _row_data(cast(Mapping[str, Any], rows[1]))
    if len(header) != len(values):
        raise RuntimeError("Athena result header does not match its data row.")

    parsed: dict[str, str | None] = {}
    for header_value, result_value in zip(header, values, strict=True):
        name = header_value.get("VarCharValue")
        if not isinstance(name, str) or not name:
            raise RuntimeError("Athena returned an invalid result column name.")
        value = result_value.get("VarCharValue")
        if value is not None and not isinstance(value, str):
            raise RuntimeError(f"Athena returned an invalid value for {name}.")
        parsed[name] = value
    return parsed


def _execute_single_row_query(
    client: AthenaQueryClient,
    sql: str,
    database: str,
    workgroup: str,
    output_location: str,
    poll_interval_seconds: float,
    timeout_seconds: float,
    clock: Clock = time.monotonic,
    sleeper: Sleeper = time.sleep,
) -> AthenaQueryResult:
    """Execute one Athena query and return its single result row."""

    if not database:
        raise ValueError("database must not be empty")
    if not workgroup:
        raise ValueError("workgroup must not be empty")
    if not output_location:
        raise ValueError("output_location must not be empty")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
    )
    query_execution_id = response.get("QueryExecutionId")
    if not isinstance(query_execution_id, str) or not query_execution_id:
        raise RuntimeError("Athena returned no query execution ID.")

    deadline = clock() + timeout_seconds
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = _query_status(response)
        state = status.get("State")
        if state == "SUCCEEDED":
            break
        if state in _FAILED_QUERY_STATES:
            reason = status.get("StateChangeReason", "no failure reason returned")
            raise RuntimeError(
                f"Athena query {query_execution_id} ended in {state}: {reason}"
            )
        if state not in _ACTIVE_QUERY_STATES:
            raise RuntimeError(
                f"Athena query {query_execution_id} returned unknown state {state!r}."
            )

        remaining_seconds = deadline - clock()
        if remaining_seconds <= 0:
            raise TimeoutError(
                f"Athena query {query_execution_id} did not finish within "
                f"{timeout_seconds} seconds."
            )
        sleeper(min(poll_interval_seconds, remaining_seconds))

    result_response = client.get_query_results(
        QueryExecutionId=query_execution_id,
        # Athena includes the header in Rows and may return a continuation token
        # when MaxResults exactly fits the header plus one data row. The spare
        # slot also lets the parser detect a second data row directly.
        MaxResults=_SINGLE_ROW_RESULT_PAGE_SIZE,
    )
    return AthenaQueryResult(
        query_execution_id=query_execution_id,
        row=_single_result_row(result_response),
    )


class AthenaQueryResource(dg.ConfigurableResource):
    """Execute bounded, single-row Athena validation queries synchronously."""

    database: str
    workgroup: str
    output_location: str
    region_name: str
    poll_interval_seconds: float = 1.0
    timeout_seconds: float = 300.0

    def query_one(self, sql: str) -> AthenaQueryResult:
        """Run a query and wait for its one-row result."""

        client = cast(
            AthenaQueryClient,
            boto3.client("athena", region_name=self.region_name),
        )
        return _execute_single_row_query(
            client=client,
            sql=sql,
            database=self.database,
            workgroup=self.workgroup,
            output_location=self.output_location,
            poll_interval_seconds=self.poll_interval_seconds,
            timeout_seconds=self.timeout_seconds,
        )


# Configured instance injected into the analytical readiness checks.
_AWS_REGION = os.environ.get("ZAVANT_AWS_REGION", "us-east-1")
_DEPLOYMENT_ENVIRONMENT = os.environ.get("ZAVANT_DEPLOYMENT_ENVIRONMENT", "prod")
_S3_BUCKET = os.environ.get("ZAVANT_S3_BUCKET", "")
_S3_PREFIX = os.environ.get("ZAVANT_S3_PREFIX", "lake").strip("/")
_ATHENA_WORKGROUP = os.environ.get("ZAVANT_ANALYTICAL_ATHENA_WORKGROUP", "primary")
_ANALYTICAL_DATABASE = f"zavant_analytical_{_DEPLOYMENT_ENVIRONMENT}"
_ATHENA_OUTPUT_LOCATION = (
    f"s3://{_S3_BUCKET}/{_S3_PREFIX}/analytical/athena-results/dagster-checks/"
    if _S3_BUCKET
    else ""
)
ANALYTICAL_READINESS_ATHENA_RESOURCE = AthenaQueryResource(
    database=_ANALYTICAL_DATABASE,
    workgroup=_ATHENA_WORKGROUP,
    output_location=_ATHENA_OUTPUT_LOCATION,
    region_name=_AWS_REGION,
    # Two source queries must fit comfortably inside a sensor evaluation.
    timeout_seconds=20,
)
