"""The two independently validated publication families in the external lake."""

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from zavant.projection.baseball_savant.contracts import (
    CURRENT_STATCAST_DATE_REVISIONS_CONTRACT,
    STATCAST_ICEBERG_CONTRACTS,
    STATCAST_PROJECTION_CONTRACT_VERSION,
)
from zavant.projection.contracts import TableContract
from zavant.projection.mlb_stats_api.contracts import (
    CURRENT_REVISION_CONTRACT,
    PROJECTION_CONTRACT_VERSION,
    TABLE_CONTRACTS,
)


CYCLE_TIMEZONE = ZoneInfo(
    os.environ.get("ZAVANT_DAGSTER_CYCLE_TIMEZONE") or "America/Los_Angeles"
)
CYCLE_TAG = "zavant/processing_cycle"
BRANCH_TAG = "zavant/dbt_branch"
PUBLICATION_TAG = "zavant/publication"
ANALYTICAL_DATABASE = (
    f"zavant_analytical_{os.environ.get('ZAVANT_DEPLOYMENT_ENVIRONMENT', 'prod')}"
)


def cycle_start(now: datetime) -> datetime:
    """Identify the local processing day, not an assumed source finish time."""

    if now.utcoffset() is None:
        raise ValueError("Processing time must be timezone-aware.")
    return now.astimezone(CYCLE_TIMEZONE).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class PublicationSource:
    """Producer-owned inventory and completion contract for one source family."""

    name: str
    raw_asset_name: str
    manifest_prefix: str
    manifest_contract: str
    mapping_table: str
    marker_table: str
    entity_key: str
    contract_version: str
    tables: tuple[TableContract, ...]


STATS_API = PublicationSource(
    name="stats_api",
    raw_asset_name="mlb_stats_api_raw",
    manifest_prefix="runs/daily/",
    manifest_contract="zavant-daily-acquisition-run/v2",
    mapping_table=CURRENT_REVISION_CONTRACT.name,
    marker_table="games",
    entity_key="game_pk",
    contract_version=PROJECTION_CONTRACT_VERSION,
    tables=(*TABLE_CONTRACTS.values(), CURRENT_REVISION_CONTRACT),
)
SAVANT = PublicationSource(
    name="savant",
    raw_asset_name="baseball_savant_raw",
    manifest_prefix="runs/baseball_savant/daily/",
    manifest_contract="baseball-savant-daily-run/v1",
    mapping_table=CURRENT_STATCAST_DATE_REVISIONS_CONTRACT.name,
    marker_table="statcast_dates",
    entity_key="game_date",
    contract_version=STATCAST_PROJECTION_CONTRACT_VERSION,
    tables=STATCAST_ICEBERG_CONTRACTS,
)
PUBLICATION_SOURCES = (STATS_API, SAVANT)
SOURCES_BY_NAME = {source.name: source for source in PUBLICATION_SOURCES}
