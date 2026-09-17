"""Producer-owned inventory shared by Glue execution and orchestration."""

from zavant.projection.baseball_savant.contracts import STATCAST_ICEBERG_CONTRACTS
from zavant.projection.contracts import TableContract
from zavant.projection.mlb_stats_api.contracts import (
    CURRENT_REVISION_CONTRACT,
    TABLE_CONTRACTS,
)


def all_projection_contracts() -> tuple[TableContract, ...]:
    """Return every Iceberg history and control table, regardless of consumers.

    Glue uses this inventory to ensure its tables exist. Dagster groups the
    same source contracts into externally published assets, including tables
    that no dbt model reads.
    Current-state views are defined separately by ``all_current_views``.
    """

    return (
        *TABLE_CONTRACTS.values(),
        CURRENT_REVISION_CONTRACT,
        *STATCAST_ICEBERG_CONTRACTS,
    )
