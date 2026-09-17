"""Shared source-readiness predicates used by monitoring and build preflight."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import dagster as dg
from pydantic import Field

from zavant.orchestration.resources.acquisition import AcquisitionEvidenceResource
from zavant.orchestration.resources.athena import AthenaQueryResource
from zavant.orchestration.sources import PublicationSource, cycle_start


READINESS_CHECK_NAME = "published_after_acquisition"


class PublicationConfig(dg.Config):
    """Pin queued work to the cycle and publications that made it eligible.

    Empty values are for manual runs: validate the current cycle's live sources.
    These IDs validate inputs; they do not provide SQL snapshot isolation.
    """

    processing_cycle: str = ""
    expected_publications: dict[str, str] = Field(default_factory=dict)


@dataclass(frozen=True)
class PublicationState:
    """Readiness evidence shared by the sensor, checks, and build preflight."""

    source: str
    ready: bool
    reason: str
    publication_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def publication_sql(database: str, source: PublicationSource) -> str:
    """Validate a source independently using its terminal revision registry.

    A single snapshot of the mapping must have one reconciliation identity.
    MAX(projected_at) on a history table would neither prove completeness nor
    identify the revision actually exposed by the current-state views.
    """

    mapping = f"{_quote_identifier(database)}.{_quote_identifier(source.mapping_table)}"
    marker = f"{_quote_identifier(database)}.{_quote_identifier(source.marker_table)}"
    entity = _quote_identifier(source.entity_key)
    version = source.contract_version.replace("'", "''")
    return f"""
WITH completed AS (
    SELECT DISTINCT {entity}, source_revision_id
    FROM {marker}
    WHERE projection_contract_version = '{version}'
)
SELECT COUNT(*) AS mapping_rows,
       COUNT(DISTINCT mapping.{entity}) AS distinct_entities,
       COUNT(DISTINCT mapping.projection_run_id) AS publication_count,
       MIN(mapping.projection_run_id) AS publication_id,
       CAST(MIN(mapping.reconciled_at) AS VARCHAR) AS reconciled_at,
       SUM(CASE WHEN mapping.projection_run_id IS NULL
                     OR mapping.reconciled_at IS NULL
                     OR mapping.projection_contract_version IS NULL
                     OR mapping.projection_contract_version <> '{version}'
                     OR completed.{entity} IS NULL
                THEN 1 ELSE 0 END) AS invalid_mappings
FROM {mapping} AS mapping
LEFT JOIN completed
  ON completed.{entity} = mapping.{entity}
 AND completed.source_revision_id = mapping.source_revision_id
""".strip()


def read_publication(
    source: PublicationSource,
    athena: AthenaQueryResource,
    acquisition: AcquisitionEvidenceResource,
    now: datetime,
) -> PublicationState:
    """Require a completed daily acquisition followed by a valid publication.

    Unchanged source content is allowed: a successful daily reconciliation is
    still usable. Historical-only/backfill runs without today's acquisition are
    deliberately not automatic build triggers. Exceptions remain visible to the
    caller rather than being mistaken for readiness.
    """

    start = cycle_start(now)
    metadata: dict[str, Any] = {
        "processing_cycle": start.date().isoformat(),
        "source_family": source.name,
    }
    evidence = acquisition.latest(source, start)
    if evidence is None:
        return PublicationState(
            source.name,
            False,
            "No acquisition attempt in this processing cycle.",
            metadata=metadata,
        )
    metadata.update(
        acquisition_run_id=evidence.run_id, manifest_uri=evidence.manifest_uri
    )
    if evidence.status != "complete" or evidence.completed_at is None:
        return PublicationState(
            source.name,
            False,
            "Latest acquisition attempt is not complete.",
            metadata=metadata,
        )
    if not start <= evidence.started_at <= evidence.completed_at <= now:
        return PublicationState(
            source.name,
            False,
            "Acquisition timestamps are inconsistent.",
            metadata=metadata,
        )
    result = athena.query_one(publication_sql(athena.database, source))
    metadata["athena_query_execution_id"] = result.query_execution_id
    row = result.row
    counts = {
        name: int(row.get(name) or "0")
        for name in (
            "mapping_rows",
            "distinct_entities",
            "publication_count",
            "invalid_mappings",
        )
    }
    metadata.update(counts)
    publication_id = row.get("publication_id") or ""
    if (
        counts["mapping_rows"] <= 0
        or counts["mapping_rows"] != counts["distinct_entities"]
        or counts["publication_count"] != 1
        or counts["invalid_mappings"] != 0
        or not publication_id
    ):
        return PublicationState(
            source.name,
            False,
            "Revision mappings are empty, mixed, duplicated, or incomplete.",
            metadata=metadata,
        )
    # Spark writes these Athena TIMESTAMP values in UTC (without a zone suffix).
    reconciled_at = datetime.fromisoformat(row.get("reconciled_at") or "")
    if reconciled_at.utcoffset() is None:
        reconciled_at = reconciled_at.replace(tzinfo=timezone.utc)
    metadata.update(
        publication_id=publication_id, reconciled_at=reconciled_at.isoformat()
    )
    if not evidence.completed_at <= reconciled_at <= now:
        return PublicationState(
            source.name,
            False,
            "Waiting for projection after the latest acquisition.",
            metadata=metadata,
        )
    return PublicationState(
        source.name,
        True,
        "Acquisition complete and current revisions published.",
        publication_id,
        metadata,
    )


def require_publications(
    sources: tuple[PublicationSource, ...],
    config: PublicationConfig,
    athena: AthenaQueryResource,
    acquisition: AcquisitionEvidenceResource,
    now: datetime,
) -> dict[str, PublicationState]:
    """Fail closed if queued work no longer matches its readiness evidence."""

    cycle = cycle_start(now).date().isoformat()
    if config.processing_cycle and config.processing_cycle != cycle:
        raise dg.Failure(
            "This run belongs to an expired processing cycle; reevaluate readiness."
        )
    states = {}
    for source in sources:
        state = read_publication(source, athena, acquisition, now)
        if not state.ready:
            raise dg.Failure(f"{source.name}: {state.reason}", metadata=state.metadata)
        expected = config.expected_publications.get(source.name)
        if expected is not None and expected != state.publication_id:
            raise dg.Failure(
                f"{source.name}: publication changed while this run was queued."
            )
        states[source.name] = state
    # One Glue run publishes both registries, sequentially. Mixed-source models
    # must not read across that short publication window or a partial failure.
    if len({state.publication_id for state in states.values()}) > 1:
        raise dg.Failure("Sources have not yet published from the same Glue run.")
    return states
