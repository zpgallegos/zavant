"""Capability protocols exposed by the MLB Stats API client."""

from datetime import date, datetime
from typing import Protocol

from zavant.ingestion.mlb_stats_api.client import RetrievedResource


class LiveGameApi(Protocol):
    """Retrieve complete live-game resources."""

    def get_live_game(self, game_pk: int) -> RetrievedResource: ...


class ScheduleApi(Protocol):
    """Retrieve bounded schedule resources."""

    def get_schedule(
        self,
        start_date: date,
        end_date: date,
        sport_id: int = 1,
    ) -> RetrievedResource: ...


class GameChangesApi(Protocol):
    """Retrieve pages from the corrected-game stream."""

    def get_game_changes(
        self,
        updated_since: datetime,
        sport_id: int = 1,
        limit: int = 1000,
        offset: int = 0,
    ) -> RetrievedResource: ...


class ScheduleAndLiveGameApi(ScheduleApi, LiveGameApi, Protocol):
    """Retrieve schedules and the live games referenced by them."""
