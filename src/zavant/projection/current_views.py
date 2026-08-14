"""Athena views exposing only the current revision of each analytical dataset."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from time import sleep
from typing import Any, Callable, Dict, Mapping, Protocol

from zavant.projection.contracts import TABLE_CONTRACTS, TableContract
from zavant.storage.s3_objects import S3ObjectBackend


PRIVATE_COLUMNS = frozenset(
    {
        "feed_timecode",
        "projection_contract_version",
        "projection_run_id",
        "projected_at",
        "raw_object_uri",
        "source_observed_at",
        "source_revision_id",
        "source_uri",
    }
)


class AthenaClient(Protocol):
    """Subset of the Boto3 Athena client used to publish current views."""

    def start_query_execution(self, **kwargs: Any) -> Dict[str, Any]:
        ...

    def get_query_execution(self, **kwargs: Any) -> Dict[str, Any]:
        ...


@dataclass(frozen=True)
class CurrentView:
    """Name and DDL for one current-state analytical view."""

    name: str
    sql: str


VIEW_PUBLICATION_KEY = "analytical/control/current-views.json"


def current_view_name(table_name: str) -> str:
    """Return the public view name for an internal history table."""

    return f"current_{table_name}"


def all_current_views(database: str) -> tuple[CurrentView, ...]:
    """Build deterministic current-state views for every analytical contract."""

    return tuple(
        CurrentView(
            name=current_view_name(contract.name),
            sql=create_current_view_sql(database, contract),
        )
        for contract in TABLE_CONTRACTS.values()
    )


def create_current_view_sql(database: str, contract: TableContract) -> str:
    """Create Athena DDL that resolves one history table through the current pointer."""

    columns = [
        f'    history."{column.name}"'
        for column in contract.columns
        if column.name not in PRIVATE_COLUMNS
    ]
    if contract.name == "games":
        columns.append('    current_revision."reconciled_at"')
        columns.append('    current_revision."source_revision_id"')
    selection = ",\n".join(columns)
    return (
        f'CREATE OR REPLACE VIEW "{current_view_name(contract.name)}" AS\n'
        f"SELECT\n{selection}\n"
        f'FROM "{database}"."{contract.name}" AS history\n'
        f'INNER JOIN "{database}"."current_game_revisions" AS current_revision\n'
        '    ON history."game_pk" = current_revision."game_pk"\n'
        '    AND history."source_revision_id" = '
        'current_revision."source_revision_id"'
    )


def publish_current_views(
    client: AthenaClient,
    database: str,
    workgroup: str,
    output_uri: str,
    poll_interval_seconds: float = 1.0,
    wait: Callable[[float], None] = sleep,
) -> None:
    """Create or replace every current view and wait for each DDL to finish.

    Raises:
        RuntimeError: If Athena rejects a view or returns an invalid response.
    """

    for view in all_current_views(database):
        response = client.start_query_execution(
            QueryString=view.sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_uri},
            WorkGroup=workgroup,
        )
        query_execution_id = response.get("QueryExecutionId")
        if not isinstance(query_execution_id, str) or not query_execution_id:
            raise RuntimeError(f"Athena did not start current view {view.name}")
        _wait_for_query(
            client,
            query_execution_id,
            view.name,
            poll_interval_seconds,
            wait,
        )


def current_views_need_publication(
    backend: S3ObjectBackend,
    database: str,
    existing_tables: set[str],
) -> bool:
    """Return whether a view is absent or its recorded definition has changed."""

    expected_names = {view.name for view in all_current_views(database)}
    if not expected_names.issubset(existing_tables):
        return True
    try:
        marker = json.loads(backend.read(VIEW_PUBLICATION_KEY))
    except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError):
        return True
    return not isinstance(marker, dict) or marker.get("fingerprint") != _fingerprint(
        database
    )


def record_current_view_publication(
    backend: S3ObjectBackend,
    database: str,
) -> None:
    """Persist the definitions successfully published by Athena."""

    marker = {
        "contract": "zavant-current-analytical-views/v1",
        "database": database,
        "fingerprint": _fingerprint(database),
        "views": [view.name for view in all_current_views(database)],
    }
    backend.overwrite(
        VIEW_PUBLICATION_KEY,
        json.dumps(marker, indent=2, sort_keys=True).encode() + b"\n",
    )


def _fingerprint(database: str) -> str:
    definitions = "\n\n".join(view.sql for view in all_current_views(database))
    return sha256(definitions.encode()).hexdigest()


def _wait_for_query(
    client: AthenaClient,
    query_execution_id: str,
    view_name: str,
    poll_interval_seconds: float,
    wait: Callable[[float], None],
) -> None:
    terminal_states = {"FAILED", "CANCELLED", "SUCCEEDED"}
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = _query_status(response)
        state = status.get("State")
        if state not in terminal_states:
            wait(poll_interval_seconds)
            continue
        if state == "SUCCEEDED":
            return
        reason = status.get("StateChangeReason", "no failure reason returned")
        raise RuntimeError(
            f"Athena failed to publish current view {view_name}: {state}: {reason}"
        )


def _query_status(response: Mapping[str, Any]) -> Mapping[str, Any]:
    query_execution = response.get("QueryExecution")
    if not isinstance(query_execution, Mapping):
        raise RuntimeError("Athena returned no query execution")
    status = query_execution.get("Status")
    if not isinstance(status, Mapping):
        raise RuntimeError("Athena returned no query status")
    return status
