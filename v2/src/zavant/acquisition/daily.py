"""Daily coordinator for schedule and correction acquisition branches."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import logging
from typing import Any, Callable, Dict, Optional
from uuid import UUID, uuid4

from zavant.acquisition.corrected_games import (
    CorrectedGameProcessingResult,
    CorrectedGameProcessor,
)
from zavant.acquisition.deferred_games import (
    DeferredGameProcessingResult,
    DeferredGameProcessor,
)
from zavant.acquisition.game_changes import (
    GameChangesPoller,
    GameChangesPollingError,
    GameChangesPollingResult,
)
from zavant.acquisition.schedule_discovery import (
    ScheduleDiscoverer,
    ScheduleDiscoveryResult,
    ScheduleWatermarkNotInitializedError,
)
from zavant.clients.mlb_stats_api import MlbStatsApiError
from zavant.contracts.game_changes import GameChangesContractError
from zavant.contracts.schedule import ScheduleContractError
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.errors import (
    DeferredGameConflictError,
    GameChangesConflictError,
    GameChangesWatermarkConflictError,
    RawGameConflictError,
    ScheduleConflictError,
    ScheduleWatermarkConflictError,
)
from zavant.storage.protocols import DailyRunStore


Clock = Callable[[], datetime]
RunIdFactory = Callable[[], UUID]
LOGGER = logging.getLogger(__name__)
BRANCH_ERRORS = (
    DeferredGameConflictError,
    GameChangesContractError,
    GameChangesConflictError,
    GameChangesPollingError,
    GameChangesWatermarkConflictError,
    MlbStatsApiError,
    OSError,
    RawGameConflictError,
    ScheduleContractError,
    ScheduleConflictError,
    ScheduleWatermarkConflictError,
    ScheduleWatermarkNotInitializedError,
    ValueError,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class DailyAcquisitionResult:
    """Aggregate result of one daily acquisition run."""

    run_id: UUID
    started_at: datetime
    through_date: date
    manifest_path: ArtifactReference
    status: str
    branch_statuses: Dict[str, str]

    @property
    def successful(self) -> bool:
        return self.status == "complete"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "branch_statuses": self.branch_statuses,
            "manifest_path": str(self.manifest_path),
            "run_id": str(self.run_id),
            "started_at": self.started_at.isoformat(),
            "status": self.status,
            "through_date": self.through_date.isoformat(),
        }


class DailyAcquisitionCoordinator:
    """Coordinate independent schedule and correction acquisition branches.

    Args:
        changes_poller: Durable corrected-game discovery service.
        corrected_game_processor: Processor for all outstanding corrections.
        deferred_game_processor: Processor for unfinished scheduled games.
        schedule_discoverer: Incremental schedule-to-game discovery service.
        run_store: Durable coordinator run-manifest store.
        clock: Function capturing the coordinator start time.
        run_id_factory: Function generating coordinator run identifiers.
    """

    def __init__(
        self,
        changes_poller: GameChangesPoller,
        corrected_game_processor: CorrectedGameProcessor,
        deferred_game_processor: DeferredGameProcessor,
        schedule_discoverer: ScheduleDiscoverer,
        run_store: DailyRunStore,
        clock: Clock = utc_now,
        run_id_factory: RunIdFactory = uuid4,
    ) -> None:
        self.changes_poller = changes_poller
        self.corrected_game_processor = corrected_game_processor
        self.deferred_game_processor = deferred_game_processor
        self.schedule_discoverer = schedule_discoverer
        self.run_store = run_store
        self.clock = clock
        self.run_id_factory = run_id_factory

    def run(
        self,
        initial_schedule_date: Optional[date] = None,
        initial_correction_watermark: Optional[datetime] = None,
        through_date: Optional[date] = None,
        schedule_lookback_days: int = 7,
        correction_overlap: timedelta = timedelta(minutes=5),
        correction_limit: int = 1000,
        correction_max_pages: int = 100,
        sport_id: int = 1,
    ) -> DailyAcquisitionResult:
        """Run correction discovery/processing and schedule discovery once.

        Branch failures are recorded independently and do not prevent later
        branches from running. Correction processing runs even when the new
        correction poll fails, allowing older durable pending work to recover.

        Args:
            initial_schedule_date: First-run inclusive schedule bootstrap date.
            initial_correction_watermark: First-run correction checkpoint.
            through_date: Inclusive schedule end date; defaults to UTC run date.
            schedule_lookback_days: Rolling schedule dates to reconsider.
            correction_overlap: Safety interval before correction watermark.
            correction_limit: Maximum source items per correction page.
            correction_max_pages: Maximum pages allowed in one correction poll.
            sport_id: MLB sport identifier, with `1` representing MLB.

        Returns:
            Aggregate status and durable coordinator manifest location.

        Raises:
            ValueError: If the coordinator clock is timezone-naive.
            DailyRunConflictError: If coordinator run persistence conflicts.
            OSError: If the coordinator manifest cannot be persisted.
        """

        started_at = self._normalize_timestamp(self.clock(), "daily clock result")
        resolved_through_date = through_date or started_at.date()
        run_id = self.run_id_factory()
        started_run = self.run_store.start(
            run_id=run_id,
            started_at=started_at,
            through_date=resolved_through_date,
            configuration={
                "correction_limit": correction_limit,
                "correction_max_pages": correction_max_pages,
                "correction_overlap_seconds": correction_overlap.total_seconds(),
                "initial_correction_watermark": (
                    initial_correction_watermark.isoformat()
                    if initial_correction_watermark is not None
                    else None
                ),
                "initial_schedule_date": (
                    initial_schedule_date.isoformat()
                    if initial_schedule_date is not None
                    else None
                ),
                "schedule_lookback_days": schedule_lookback_days,
                "sport_id": sport_id,
            },
        )
        LOGGER.info(
            "daily acquisition started run_id=%s through_date=%s manifest=%s",
            run_id,
            resolved_through_date,
            started_run.manifest_path,
        )

        self._run_correction_discovery(
            manifest_path=started_run.manifest_path,
            initial_watermark=initial_correction_watermark,
            sport_id=sport_id,
            limit=correction_limit,
            overlap=correction_overlap,
            max_pages=correction_max_pages,
        )
        self._run_correction_processing(started_run.manifest_path)
        self._run_deferred_game_processing(started_run.manifest_path)
        self._run_schedule_discovery(
            manifest_path=started_run.manifest_path,
            initial_start_date=initial_schedule_date,
            through_date=resolved_through_date,
            lookback_days=schedule_lookback_days,
            sport_id=sport_id,
        )
        branch_statuses = self.run_store.finalize(started_run.manifest_path)
        status = (
            "failed"
            if any(value == "failed" for value in branch_statuses.values())
            else "complete"
        )
        LOGGER.info(
            "daily acquisition finished run_id=%s status=%s branches=%s",
            run_id,
            status,
            branch_statuses,
        )
        return DailyAcquisitionResult(
            run_id=run_id,
            started_at=started_at,
            through_date=resolved_through_date,
            manifest_path=started_run.manifest_path,
            status=status,
            branch_statuses=branch_statuses,
        )

    def _run_correction_discovery(
        self,
        manifest_path: ArtifactReference,
        initial_watermark: Optional[datetime],
        sport_id: int,
        limit: int,
        overlap: timedelta,
        max_pages: int,
    ) -> None:
        LOGGER.info("daily branch started branch=correction_discovery")
        try:
            result: GameChangesPollingResult = self.changes_poller.poll(
                initial_watermark=initial_watermark,
                sport_id=sport_id,
                limit=limit,
                overlap=overlap,
                max_pages=max_pages,
            )
        except BRANCH_ERRORS as exc:
            self._record_error(manifest_path, "correction_discovery", exc)
            return
        self.run_store.record_branch(
            manifest_path,
            "correction_discovery",
            "complete",
            result.as_dict(),
        )
        LOGGER.info("daily branch finished branch=correction_discovery status=complete")

    def _run_correction_processing(self, manifest_path: ArtifactReference) -> None:
        LOGGER.info("daily branch started branch=correction_processing")
        try:
            result: CorrectedGameProcessingResult = (
                self.corrected_game_processor.process_all()
            )
        except BRANCH_ERRORS as exc:
            self._record_error(manifest_path, "correction_processing", exc)
            return
        self.run_store.record_branch(
            manifest_path,
            "correction_processing",
            "complete" if result.successful else "failed",
            result.as_dict(),
        )
        LOGGER.info(
            "daily branch finished branch=correction_processing status=%s",
            "complete" if result.successful else "failed",
        )

    def _run_schedule_discovery(
        self,
        manifest_path: ArtifactReference,
        initial_start_date: Optional[date],
        through_date: date,
        lookback_days: int,
        sport_id: int,
    ) -> None:
        LOGGER.info("daily branch started branch=schedule_discovery")
        try:
            result: ScheduleDiscoveryResult = self.schedule_discoverer.discover(
                initial_start_date=initial_start_date,
                through_date=through_date,
                lookback_days=lookback_days,
                sport_id=sport_id,
            )
        except BRANCH_ERRORS as exc:
            self._record_error(manifest_path, "schedule_discovery", exc)
            return
        self.run_store.record_branch(
            manifest_path,
            "schedule_discovery",
            result.status
            if result.status == "skipped"
            else ("complete" if result.successful else "failed"),
            result.as_dict(),
        )
        LOGGER.info(
            "daily branch finished branch=schedule_discovery status=%s",
            result.status
            if result.status == "skipped"
            else ("complete" if result.successful else "failed"),
        )

    def _run_deferred_game_processing(
        self,
        manifest_path: ArtifactReference,
    ) -> None:
        LOGGER.info("daily branch started branch=deferred_game_processing")
        try:
            result: DeferredGameProcessingResult = (
                self.deferred_game_processor.process_all()
            )
        except BRANCH_ERRORS as exc:
            self._record_error(manifest_path, "deferred_game_processing", exc)
            return
        status = "complete" if result.successful else "failed"
        self.run_store.record_branch(
            manifest_path,
            "deferred_game_processing",
            status,
            result.as_dict(),
        )
        LOGGER.info(
            "daily branch finished branch=deferred_game_processing status=%s",
            status,
        )

    def _record_error(
        self, manifest_path: ArtifactReference, branch: str, exc: Exception
    ) -> None:
        LOGGER.info(
            "daily branch failed branch=%s error_type=%s error=%s",
            branch,
            type(exc).__name__,
            str(exc)[:500],
        )
        self.run_store.record_branch(
            manifest_path,
            branch,
            "failed",
            {
                "error_message": str(exc)[:500],
                "error_type": type(exc).__name__,
            },
        )

    @staticmethod
    def _normalize_timestamp(value: datetime, name: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must include a UTC offset")
        return value.astimezone(timezone.utc)
