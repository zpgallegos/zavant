"""Small composition surface for acquisition applications."""

from datetime import datetime
from typing import Callable, Protocol

from zavant.acquisition.bounded_games import BoundedGameAcquirer, MlbGameAcquisitionApi
from zavant.acquisition.corrected_games import CorrectedGameProcessor, MlbCorrectedGameApi
from zavant.acquisition.daily import DailyAcquisitionCoordinator, utc_now
from zavant.acquisition.game_changes import GameChangesPoller, MlbGameChangesApi
from zavant.acquisition.schedule_discovery import ScheduleDiscoverer
from zavant.acquisition.season_backfill import (
    MlbSeasonBackfillApi,
    SeasonBackfillCoordinator,
)
from zavant.storage.bundles import AcquisitionStorage


Clock = Callable[[], datetime]


class MlbDailyApi(
    MlbGameAcquisitionApi,
    MlbCorrectedGameApi,
    MlbGameChangesApi,
    Protocol,
):
    """Complete MLB client surface required by the daily workflow."""


def build_daily_coordinator(
    api: MlbDailyApi,
    storage: AcquisitionStorage,
    clock: Clock = utc_now,
) -> DailyAcquisitionCoordinator:
    bounded_acquirer = BoundedGameAcquirer(
        api=api,
        schedule_store=storage.schedules,
        game_store=storage.raw_games,
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
