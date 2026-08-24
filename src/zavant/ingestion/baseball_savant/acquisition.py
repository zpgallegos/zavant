"""Shared one-date acquisition primitive for Baseball Savant workflows."""

from datetime import date
from typing import Protocol
from uuid import UUID

from zavant.ingestion.baseball_savant.contract import StatcastCsvResponse
from zavant.ingestion.baseball_savant.storage import (
    BaseballSavantRawStore,
    LandedStatcastDate,
)
from zavant.ingestion.http import RetrievedResource


class BaseballSavantApi(Protocol):
    """Source surface shared by scheduled and historical acquisition."""

    def get_statcast_date(self, game_date: date) -> RetrievedResource: ...


def acquire_statcast_date(
    api: BaseballSavantApi,
    store: BaseballSavantRawStore,
    game_date: date,
    run_id: UUID,
) -> LandedStatcastDate:
    """Retrieve, validate, and immutably land one exact-date CSV export."""

    retrieved = api.get_statcast_date(game_date)
    response = StatcastCsvResponse.from_bytes(retrieved.body, game_date)
    return store.land_date(
        response=response,
        raw=retrieved.body,
        source_uri=retrieved.source_uri,
        run_id=run_id,
    )
