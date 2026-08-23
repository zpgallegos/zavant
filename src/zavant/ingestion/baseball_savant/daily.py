"""Daily, date-bounded acquisition for Baseball Savant Statcast CSV data."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
from typing import Any, Callable, Dict, Optional, Protocol, Tuple
from uuid import UUID, uuid4

from zavant._time import Clock, as_utc, utc_now
from zavant.ingestion.baseball_savant.client import BaseballSavantError
from zavant.ingestion.http import RetrievedResource
from zavant.ingestion.baseball_savant.contract import (
    BaseballSavantContractError,
    StatcastCsvResponse,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.ingestion.baseball_savant.storage import (
    BaseballSavantStorageError,
    BaseballSavantRawStore,
    BaseballSavantStore,
    LandedStatcastDate,
)


LOGGER = logging.getLogger(__name__)
RunIdFactory = Callable[[], UUID]


class BaseballSavantApi(Protocol):
    """Source surface required by the Savant daily acquisition process."""

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


@dataclass(frozen=True)
class BaseballSavantDailyResult:
    """Aggregate outcome of one Savant daily acquisition attempt."""

    run_id: UUID
    started_at: datetime
    through_date: date
    planned_dates: Tuple[date, ...]
    manifest_path: ArtifactReference
    status: str
    succeeded: int
    failed: int

    @property
    def successful(self) -> bool:
        return self.status == "complete"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "failed": self.failed,
            "manifest_path": str(self.manifest_path),
            "planned_dates": [value.isoformat() for value in self.planned_dates],
            "run_id": str(self.run_id),
            "started_at": self.started_at.isoformat(),
            "status": self.status,
            "succeeded": self.succeeded,
            "through_date": self.through_date.isoformat(),
        }


class BaseballSavantDailyAcquirer:
    """Own the daily workflow for all-player, one-date Savant snapshots.

    This source remains separate from Stats API orchestration because its unit
    of revision, watermark, and failure recovery is a whole date rather than a
    game.
    """

    def __init__(
        self,
        api: BaseballSavantApi,
        store: BaseballSavantStore,
        clock: Clock = utc_now,
        run_id_factory: RunIdFactory = uuid4,
    ) -> None:
        self.api = api
        self.store = store
        self.clock = clock
        self.run_id_factory = run_id_factory

    def run(
        self,
        initial_date: Optional[date],
        through_date: Optional[date] = None,
        lookback_days: int = 7,
        max_dates_per_run: int = 31,
    ) -> BaseballSavantDailyResult:
        """Acquire a contiguous catch-up or rolling reconciliation window.

        The ordinary scheduled boundary is yesterday in UTC, avoiding an
        incomplete current-day export. Every source request is still bounded
        to exactly one date and issued sequentially.
        """

        if lookback_days <= 0:
            raise ValueError("lookback_days must be positive")
        if max_dates_per_run <= 0:
            raise ValueError("max_dates_per_run must be positive")
        if initial_date is not None and type(initial_date) is not date:
            raise ValueError("initial_date must be a date")
        if through_date is not None and type(through_date) is not date:
            raise ValueError("through_date must be a date")
        started_at = as_utc(self.clock(), "daily Savant clock result")
        resolved_through_date = through_date or (started_at.date() - timedelta(days=1))
        if initial_date is not None and resolved_through_date < initial_date:
            raise ValueError("through_date must not be before initial_date")
        watermark = self.store.read_watermark()
        current_through_date = watermark.through_date if watermark is not None else None
        planned_dates = self._planned_dates(
            initial_date=initial_date,
            current_through_date=current_through_date,
            through_date=resolved_through_date,
            lookback_days=lookback_days,
        )
        if len(planned_dates) > max_dates_per_run:
            raise ValueError(
                "Savant daily date range exceeds max_dates_per_run; "
                "use a bounded historical backfill before resuming daily acquisition"
            )

        run_id = self.run_id_factory()
        manifest_path = self.store.start_run(
            run_id=run_id,
            started_at=started_at,
            through_date=resolved_through_date,
            planned_dates=planned_dates,
            configuration={
                "initial_date": initial_date.isoformat() if initial_date else None,
                "lookback_days": lookback_days,
                "max_dates_per_run": max_dates_per_run,
                "request_shape": "all-players-one-date",
            },
        )
        LOGGER.info(
            "Baseball Savant daily acquisition started run_id=%s dates=%s..%s",
            run_id,
            planned_dates[0],
            planned_dates[-1],
        )
        for game_date in planned_dates:
            self._acquire_date(game_date, run_id, manifest_path)

        counts = self.store.finalize_run(manifest_path)
        status = "failed" if counts["failed"] else "complete"
        if status == "complete" and (
            current_through_date is None
            or resolved_through_date > current_through_date
        ):
            # Only a manifest with every planned date recorded successfully may
            # authorize the durable through-date to move forward.
            self.store.advance_watermark(
                expected_current=current_through_date,
                through_date=resolved_through_date,
                run_id=run_id,
                manifest_path=manifest_path,
            )
        LOGGER.info(
            "Baseball Savant daily acquisition finished run_id=%s status=%s "
            "succeeded=%d failed=%d",
            run_id,
            status,
            counts["succeeded"],
            counts["failed"],
        )
        return BaseballSavantDailyResult(
            run_id=run_id,
            started_at=started_at,
            through_date=resolved_through_date,
            planned_dates=planned_dates,
            manifest_path=manifest_path,
            status=status,
            succeeded=counts["succeeded"],
            failed=counts["failed"],
        )

    def _acquire_date(
        self,
        game_date: date,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> None:
        try:
            landed = acquire_statcast_date(
                self.api,
                self.store,
                game_date,
                run_id,
            )
        except (
            BaseballSavantContractError,
            BaseballSavantError,
            BaseballSavantStorageError,
            OSError,
        ) as exc:
            LOGGER.exception(
                "Baseball Savant date acquisition failed game_date=%s", game_date
            )
            self.store.record_date(
                manifest_path,
                game_date,
                "failed",
                {"error": str(exc), "error_type": type(exc).__name__},
            )
            return
        self.store.record_date(
            manifest_path,
            game_date,
            "succeeded",
            landed.as_dict(),
        )

    @staticmethod
    def _planned_dates(
        initial_date: Optional[date],
        current_through_date: Optional[date],
        through_date: date,
        lookback_days: int,
    ) -> Tuple[date, ...]:
        if current_through_date is None:
            if initial_date is None:
                raise ValueError(
                    "initial_date is required before the Savant watermark exists"
                )
            start_date = initial_date
        else:
            new_start = current_through_date + timedelta(days=1)
            lookback_start = through_date - timedelta(days=lookback_days - 1)
            # Always replay the rolling window, even when there are no unseen
            # dates, because Savant can revise a prior date's CSV in place.
            start_date = min(new_start, lookback_start)
            if initial_date is not None:
                start_date = max(start_date, initial_date)
        if start_date > through_date:
            start_date = through_date
        return tuple(
            start_date + timedelta(days=offset)
            for offset in range((through_date - start_date).days + 1)
        )
