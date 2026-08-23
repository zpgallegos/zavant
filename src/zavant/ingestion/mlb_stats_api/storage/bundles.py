"""Composition helpers for complete Stats API storage backends."""

from dataclasses import dataclass
from pathlib import Path
from typing import cast

from zavant._time import Clock, utc_now
from zavant.ingestion.mlb_stats_api.storage.path_daily_runs import PathDailyRunStore
from zavant.ingestion.mlb_stats_api.storage.path_deferred_games import PathDeferredGameStore
from zavant.ingestion.mlb_stats_api.storage.path_game_changes import PathGameChangesStore
from zavant.ingestion.mlb_stats_api.storage.path_game_changes_watermark import (
    PathGameChangesWatermarkStore,
)
from zavant.ingestion.mlb_stats_api.storage.path_raw import PathRawGameStore
from zavant.ingestion.mlb_stats_api.storage.path_schedule import PathScheduleStore
from zavant.ingestion.mlb_stats_api.storage.path_season_backfills import PathSeasonBackfillStore
from zavant.ingestion.mlb_stats_api.storage.path_schedule_watermark import PathScheduleWatermarkStore
from zavant.ingestion.mlb_stats_api.storage.protocols import (
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


@dataclass(frozen=True)
class AcquisitionStorage:
    """Authoritative inventory of storage required by Stats API acquisition.

    Application composition consumes this bundle so local and production
    deployments cannot accidentally be wired with different capabilities.
    """

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
    """Build filesystem storage for CLI workflows and local development."""
    return _acquisition_storage(data_dir, clock)


def _acquisition_storage(root: Path, clock: Clock) -> AcquisitionStorage:
    # Keep the capability list in one place; only the path implementation
    # differs between the local filesystem and S3.
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
    return _acquisition_storage(root, clock)
