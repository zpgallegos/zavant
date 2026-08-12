"""Composition helpers for complete acquisition storage backends."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, cast

from zavant.storage.path_daily_runs import PathDailyRunStore
from zavant.storage.path_deferred_games import PathDeferredGameStore
from zavant.storage.path_game_changes import PathGameChangesStore
from zavant.storage.path_game_changes_watermark import (
    PathGameChangesWatermarkStore,
)
from zavant.storage.path_raw import PathRawGameStore
from zavant.storage.path_schedule import PathScheduleStore
from zavant.storage.path_season_backfills import PathSeasonBackfillStore
from zavant.storage.path_schedule_watermark import PathScheduleWatermarkStore
from zavant.storage.protocols import (
    DailyRunStore,
    DeferredGameStore,
    GameChangesStore,
    GameChangesWatermarkStore,
    RawGameStore,
    ScheduleStore,
    ScheduleWatermarkStore,
    SeasonBackfillStore,
)
from zavant.storage.s3_objects import S3Client, S3ObjectBackend


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class AcquisitionStorage:
    """Complete set of storage capabilities required by acquisition."""

    raw_games: RawGameStore
    schedules: ScheduleStore
    deferred_games: DeferredGameStore
    game_changes: GameChangesStore
    schedule_watermark: ScheduleWatermarkStore
    game_changes_watermark: GameChangesWatermarkStore
    daily_runs: DailyRunStore
    season_backfills: SeasonBackfillStore


def local_acquisition_storage(
    data_dir: Path,
    clock: Clock = utc_now,
) -> AcquisitionStorage:
    """Build acquisition storage over a local lake root."""
    return AcquisitionStorage(
        raw_games=PathRawGameStore(data_dir, clock=clock),
        schedules=PathScheduleStore(data_dir, clock=clock),
        deferred_games=PathDeferredGameStore(data_dir, clock=clock),
        game_changes=PathGameChangesStore(data_dir, clock=clock),
        schedule_watermark=PathScheduleWatermarkStore(data_dir, clock=clock),
        game_changes_watermark=PathGameChangesWatermarkStore(data_dir, clock=clock),
        daily_runs=PathDailyRunStore(data_dir, clock=clock),
        season_backfills=PathSeasonBackfillStore(data_dir, clock=clock),
    )


def s3_acquisition_storage(
    client: S3Client,
    bucket: str,
    prefix: str,
    clock: Clock = utc_now,
) -> AcquisitionStorage:
    """Build acquisition stores backed by one conditionally written S3 prefix.

    The established persistence state machines operate on a deliberately small
    path surface. The S3 path implementation supplies exact reads, prefix
    listing, artifact URIs, and ETag-based conditional publication, allowing
    local and S3 backends to share all domain state transitions.

    Args:
        client: Boto3-compatible S3 client.
        bucket: Durable raw-lake bucket.
        prefix: Logical lake prefix within the bucket.
        clock: Time source shared by every storage capability.

    Returns:
        S3-backed storage capabilities for acquisition composition.
    """

    root = cast(Path, S3ObjectBackend(client, bucket, prefix).root())
    return AcquisitionStorage(
        raw_games=PathRawGameStore(root, clock=clock),
        schedules=PathScheduleStore(root, clock=clock),
        deferred_games=PathDeferredGameStore(root, clock=clock),
        game_changes=PathGameChangesStore(root, clock=clock),
        schedule_watermark=PathScheduleWatermarkStore(root, clock=clock),
        game_changes_watermark=PathGameChangesWatermarkStore(root, clock=clock),
        daily_runs=PathDailyRunStore(root, clock=clock),
        season_backfills=PathSeasonBackfillStore(root, clock=clock),
    )
