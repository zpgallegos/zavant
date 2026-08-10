"""Daily coordinator for schedule and correction acquisition branches."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from uuid import UUID, uuid4

from zavant.acquisition.corrected_games import (
    CorrectedGameProcessingResult,
    CorrectedGameProcessor,
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
from zavant.storage.local_daily_runs import LocalDailyRunStore
from zavant.storage.local_game_changes import GameChangesConflictError
from zavant.storage.local_game_changes_watermark import (
    GameChangesWatermarkConflictError,
)
from zavant.storage.local_raw import RawGameConflictError
from zavant.storage.local_schedule import ScheduleConflictError
from zavant.storage.local_schedule_watermark import ScheduleWatermarkConflictError


Clock = Callable[[], datetime]
RunIdFactory = Callable[[], UUID]
BRANCH_ERRORS = (
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
    """Return the current UTC time.

    Returns:
        A timezone-aware UTC timestamp.
    """

    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class DailyAcquisitionResult:
    """Aggregate result of one daily local acquisition run.

    Attributes:
        run_id: Unique coordinator run identifier.
        started_at: UTC timestamp captured before any branch.
        through_date: Inclusive schedule discovery end date.
        manifest_path: Durable run manifest containing every branch outcome.
        status: Complete only when every branch succeeds or is current.
        branch_statuses: Status keyed by coordinator branch.
    """

    run_id: UUID
    started_at: datetime
    through_date: date
    manifest_path: Path
    status: str
    branch_statuses: Dict[str, str]

    @property
    def successful(self) -> bool:
        """Return whether the complete daily workflow succeeded.

        Returns:
            `True` only when the aggregate status is complete.
        """

        return self.status == "complete"

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable daily run result.

        Returns:
            Run identity, status, branch states, and manifest location.
        """

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
        schedule_discoverer: Incremental schedule-to-game discovery service.
        run_store: Durable coordinator run-manifest store.
        clock: Function capturing the coordinator start time.
        run_id_factory: Function generating coordinator run identifiers.
    """

    def __init__(
        self,
        changes_poller: GameChangesPoller,
        corrected_game_processor: CorrectedGameProcessor,
        schedule_discoverer: ScheduleDiscoverer,
        run_store: LocalDailyRunStore,
        clock: Clock = utc_now,
        run_id_factory: RunIdFactory = uuid4,
    ) -> None:
        """Initialize the daily acquisition coordinator.

        Args:
            changes_poller: Durable corrected-game discovery service.
            corrected_game_processor: Processor for all outstanding corrections.
            schedule_discoverer: Incremental schedule-to-game discovery service.
            run_store: Durable coordinator run-manifest store.
            clock: Function capturing the coordinator start time.
            run_id_factory: Function generating coordinator run identifiers.
        """

        self.changes_poller = changes_poller
        self.corrected_game_processor = corrected_game_processor
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

        self._run_correction_discovery(
            manifest_path=started_run.manifest_path,
            initial_watermark=initial_correction_watermark,
            sport_id=sport_id,
            limit=correction_limit,
            overlap=correction_overlap,
            max_pages=correction_max_pages,
        )
        self._run_correction_processing(started_run.manifest_path)
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
        manifest_path: Path,
        initial_watermark: Optional[datetime],
        sport_id: int,
        limit: int,
        overlap: timedelta,
        max_pages: int,
    ) -> None:
        """Run and record the correction discovery branch.

        Args:
            manifest_path: Daily manifest receiving the outcome.
            initial_watermark: Optional first-run correction checkpoint.
            sport_id: MLB sport identifier.
            limit: Correction source page size.
            overlap: Correction query safety interval.
            max_pages: Maximum pages allowed in the poll.
        """

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

    def _run_correction_processing(self, manifest_path: Path) -> None:
        """Run and record processing of all durable correction work.

        Args:
            manifest_path: Daily manifest receiving the outcome.
        """

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

    def _run_schedule_discovery(
        self,
        manifest_path: Path,
        initial_start_date: Optional[date],
        through_date: date,
        lookback_days: int,
        sport_id: int,
    ) -> None:
        """Run and record incremental schedule discovery.

        Args:
            manifest_path: Daily manifest receiving the outcome.
            initial_start_date: Optional first-run schedule start date.
            through_date: Inclusive schedule discovery end date.
            lookback_days: Rolling schedule dates to reconsider.
            sport_id: MLB sport identifier.
        """

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

    def _record_error(self, manifest_path: Path, branch: str, exc: Exception) -> None:
        """Record a bounded branch failure in the daily manifest.

        Args:
            manifest_path: Daily manifest receiving the failure.
            branch: Coordinator branch that failed.
            exc: Operational exception raised by the branch.
        """

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
        """Validate and normalize one timestamp to UTC.

        Args:
            value: Candidate timestamp.
            name: Field name used in validation errors.

        Returns:
            Timezone-aware UTC timestamp.

        Raises:
            ValueError: If the timestamp is timezone-naive.
        """

        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must include a UTC offset")
        return value.astimezone(timezone.utc)
