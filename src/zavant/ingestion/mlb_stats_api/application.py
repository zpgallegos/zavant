"""Composition surface for MLB Stats API acquisition applications."""

from typing import Protocol

from zavant._time import Clock, utc_now
from zavant.ingestion.mlb_stats_api.acquisition.bounded_games import BoundedGameAcquirer
from zavant.ingestion.mlb_stats_api.acquisition.corrected_games import CorrectedGameProcessor
from zavant.ingestion.mlb_stats_api.acquisition.daily import DailyAcquisitionCoordinator
from zavant.ingestion.mlb_stats_api.acquisition.deferred_games import DeferredGameProcessor
from zavant.ingestion.mlb_stats_api.acquisition.game_changes import GameChangesPoller
from zavant.ingestion.mlb_stats_api.acquisition.protocols import (
    GameChangesApi,
    ScheduleAndLiveGameApi,
)
from zavant.ingestion.mlb_stats_api.acquisition.schedule_discovery import ScheduleDiscoverer
from zavant.ingestion.mlb_stats_api.acquisition.season_backfill import (
    MlbSeasonBackfillApi,
    SeasonBackfillCoordinator,
)
from zavant.ingestion.mlb_stats_api.storage.bundles import AcquisitionStorage


class MlbDailyApi(
    ScheduleAndLiveGameApi,
    GameChangesApi,
    Protocol,
):
    """Complete MLB client surface required by the daily workflow."""


def build_daily_coordinator(
    api: MlbDailyApi,
    storage: AcquisitionStorage,
    clock: Clock = utc_now,
) -> DailyAcquisitionCoordinator:
    """Compose the complete production or local Stats API daily workflow."""

    bounded_acquirer = BoundedGameAcquirer(
        api=api,
        schedule_store=storage.schedules,
        game_store=storage.raw_games,
        deferred_game_store=storage.deferred_games,
        clock=clock,
    )
    return DailyAcquisitionCoordinator(
        changes_poller=GameChangesPoller(
            api=api,
            changes_store=storage.game_changes,
            watermark_store=storage.game_changes_watermark,
            clock=clock,
        ),
        corrected_game_processor=CorrectedGameProcessor(
            api=api,
            changes_store=storage.game_changes,
            game_store=storage.raw_games,
        ),
        deferred_game_processor=DeferredGameProcessor(
            api=api,
            deferred_game_store=storage.deferred_games,
            game_store=storage.raw_games,
        ),
        schedule_discoverer=ScheduleDiscoverer(
            acquirer=bounded_acquirer,
            watermark_store=storage.schedule_watermark,
            clock=clock,
        ),
        run_store=storage.daily_runs,
        clock=clock,
    )


def build_season_backfill_coordinator(
    api: MlbSeasonBackfillApi,
    storage: AcquisitionStorage,
    clock: Clock = utc_now,
) -> SeasonBackfillCoordinator:
    """Compose historical reconciliation over either storage backend."""
    return SeasonBackfillCoordinator(
        api=api,
        schedule_store=storage.schedules,
        game_store=storage.raw_games,
        backfill_store=storage.season_backfills,
        clock=clock,
    )
