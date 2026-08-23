"""Composition surface for Baseball Savant acquisition applications."""

from zavant._time import Clock, utc_now
from zavant.ingestion.baseball_savant.backfill import (
    BaseballSavantBackfillCoordinator,
)
from zavant.ingestion.baseball_savant.backfill_storage import (
    BaseballSavantBackfillStore,
)
from zavant.ingestion.baseball_savant.daily import BaseballSavantApi
from zavant.ingestion.baseball_savant.storage import BaseballSavantRawStore


def build_backfill_coordinator(
    api: BaseballSavantApi,
    raw_store: BaseballSavantRawStore,
    backfill_store: BaseballSavantBackfillStore,
    clock: Clock = utc_now,
) -> BaseballSavantBackfillCoordinator:
    """Compose a bounded backfill over explicit persistence surfaces."""

    return BaseballSavantBackfillCoordinator(
        api=api,
        raw_store=raw_store,
        backfill_store=backfill_store,
        clock=clock,
    )
