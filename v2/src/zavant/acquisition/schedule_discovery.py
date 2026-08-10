"""Incremental schedule discovery with a durable through-date."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional
from uuid import UUID, uuid4

from zavant.acquisition.bounded_games import (
    BoundedGameAcquirer,
    BoundedGameAcquisitionResult,
)
from zavant.storage.local_schedule_watermark import LocalScheduleWatermarkStore


Clock = Callable[[], datetime]
RunIdFactory = Callable[[], UUID]


def utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        A timezone-aware UTC timestamp.
    """

    return datetime.now(timezone.utc)


class ScheduleWatermarkNotInitializedError(RuntimeError):
    """Raised when first schedule discovery omits its bootstrap start date."""


@dataclass(frozen=True)
class ScheduleDiscoveryResult:
    """Summary of one incremental schedule-discovery invocation.

    Attributes:
        status: Complete, failed, or skipped when already current.
        start_date: Inclusive schedule query start, if a query was needed.
        through_date: Requested and successfully eligible end date.
        watermark_before: Prior durable through-date, if initialized.
        watermark_after: Durable through-date after this invocation.
        acquisition: Bounded acquisition result, if a query was needed.
    """

    status: str
    start_date: Optional[date]
    through_date: date
    watermark_before: Optional[date]
    watermark_after: Optional[date]
    acquisition: Optional[BoundedGameAcquisitionResult]

    @property
    def successful(self) -> bool:
        """Return whether discovery completed or required no work.

        Returns:
            `True` for complete and skipped invocations.
        """

        return self.status in {"complete", "skipped"}

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable discovery result.

        Returns:
            Query boundaries, watermark state, and acquisition details.
        """

        return {
            "acquisition": (
                self.acquisition.as_dict() if self.acquisition is not None else None
            ),
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "status": self.status,
            "through_date": self.through_date.isoformat(),
            "watermark_after": (
                self.watermark_after.isoformat() if self.watermark_after else None
            ),
            "watermark_before": (
                self.watermark_before.isoformat() if self.watermark_before else None
            ),
        }


class ScheduleDiscoverer:
    """Run bounded acquisition over a rolling, success-watermarked window.

    Args:
        acquirer: Existing bounded schedule-to-game workflow.
        watermark_store: Durable schedule through-date store.
        clock: Function capturing the discovery request timestamp.
        run_id_factory: Function generating schedule run identifiers.
    """

    def __init__(
        self,
        acquirer: BoundedGameAcquirer,
        watermark_store: LocalScheduleWatermarkStore,
        clock: Clock = utc_now,
        run_id_factory: RunIdFactory = uuid4,
    ) -> None:
        """Initialize incremental schedule discovery.

        Args:
            acquirer: Existing bounded schedule-to-game workflow.
            watermark_store: Durable schedule through-date store.
            clock: Function capturing the discovery request timestamp.
            run_id_factory: Function generating schedule run identifiers.
        """

        self.acquirer = acquirer
        self.watermark_store = watermark_store
        self.clock = clock
        self.run_id_factory = run_id_factory

    def discover(
        self,
        initial_start_date: Optional[date] = None,
        through_date: Optional[date] = None,
        lookback_days: int = 7,
        sport_id: int = 1,
    ) -> ScheduleDiscoveryResult:
        """Discover and acquire games through a bounded calendar date.

        A rolling lookback reconsiders recent deferred or changed schedule
        entries. Previously landed final games are resolved locally by the
        bounded acquirer and do not cause another live-feed request.

        Args:
            initial_start_date: Required first-run inclusive schedule date.
            through_date: Inclusive end date. Defaults to the UTC run date.
            lookback_days: Calendar days reconsidered before the prior
                successful through-date, including that boundary date.
            sport_id: MLB sport identifier, with `1` representing MLB.

        Returns:
            Discovery status, query boundaries, and bounded acquisition result.

        Raises:
            ValueError: If dates, lookback, or timestamps are invalid.
            ScheduleWatermarkNotInitializedError: If bootstrap state is absent.
            MlbStatsApiError: If schedule retrieval cannot complete.
            ScheduleContractError: If schedule source bytes are invalid.
            ScheduleConflictError: If stored schedule evidence conflicts.
            ScheduleWatermarkConflictError: If state changes during discovery.
            OSError: If evidence or state cannot be read or written.
        """

        requested_at = self._normalize_timestamp(self.clock(), "discovery clock")
        resolved_through_date = through_date or requested_at.date()
        if type(lookback_days) is not int or lookback_days <= 0:
            raise ValueError("lookback_days must be a positive integer")
        current = self.watermark_store.read()
        if current is None:
            if initial_start_date is None:
                raise ScheduleWatermarkNotInitializedError(
                    "the first daily run requires --initial-schedule-date"
                )
            if initial_start_date > resolved_through_date:
                raise ValueError("initial_start_date must not be after through_date")
            start_date = initial_start_date
            watermark_before: Optional[date] = None
            expected_current: Optional[date] = None
            advanced_from = initial_start_date
        else:
            if initial_start_date is not None:
                raise ValueError(
                    "initial_start_date must be omitted after initialization"
                )
            watermark_before = current.through_date
            expected_current = current.through_date
            advanced_from = current.through_date
            if current.through_date >= resolved_through_date:
                return ScheduleDiscoveryResult(
                    status="skipped",
                    start_date=None,
                    through_date=resolved_through_date,
                    watermark_before=current.through_date,
                    watermark_after=current.through_date,
                    acquisition=None,
                )
            start_date = current.through_date - timedelta(days=lookback_days - 1)

        run_id = self.run_id_factory()
        acquisition = self.acquirer.acquire(
            start_date=start_date,
            end_date=resolved_through_date,
            sport_id=sport_id,
            run_id=run_id,
            requested_at=requested_at,
        )
        if not acquisition.successful:
            return ScheduleDiscoveryResult(
                status="failed",
                start_date=start_date,
                through_date=resolved_through_date,
                watermark_before=watermark_before,
                watermark_after=watermark_before,
                acquisition=acquisition,
            )

        watermark = self.watermark_store.advance(
            expected_current=expected_current,
            advanced_from=advanced_from,
            through_date=resolved_through_date,
            run_id=run_id,
            manifest_path=acquisition.manifest_path,
        )
        return ScheduleDiscoveryResult(
            status="complete",
            start_date=start_date,
            through_date=resolved_through_date,
            watermark_before=watermark_before,
            watermark_after=watermark.through_date,
            acquisition=acquisition,
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
