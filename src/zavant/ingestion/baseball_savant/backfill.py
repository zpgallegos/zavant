"""Resumable local backfill for Baseball Savant exact-date CSV exports."""

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
import logging
import time
from typing import Any, Callable, Dict, Optional, Tuple
from uuid import UUID, uuid4

from zavant._time import Clock, as_utc, utc_now
from zavant.ingestion.baseball_savant.daily import (
    BaseballSavantApi,
    acquire_statcast_date,
)
from zavant.ingestion.baseball_savant.client import BaseballSavantError
from zavant.ingestion.baseball_savant.contract import BaseballSavantContractError
from zavant.storage.artifacts import ArtifactReference
from zavant.ingestion.baseball_savant.storage import (
    BaseballSavantRawStore,
    BaseballSavantStorageError,
)
from zavant.ingestion.baseball_savant.backfill_storage import BaseballSavantBackfillStore


LOGGER = logging.getLogger(__name__)
RunIdFactory = Callable[[], UUID]
Sleeper = Callable[[float], None]


class BaseballSavantBackfillMode(str, Enum):
    """Selection strategy for dates that already have a current revision."""

    MISSING = "missing"
    VERIFY = "verify"


@dataclass(frozen=True)
class BaseballSavantBackfillResult:
    """Durable result of one bounded Savant historical backfill."""

    run_id: UUID
    started_at: datetime
    start_date: date
    end_date: date
    manifest_path: ArtifactReference
    mode: BaseballSavantBackfillMode
    dry_run: bool
    resumed: bool
    succeeded: int
    skipped: int
    failed: int

    @property
    def successful(self) -> bool:
        return self.failed == 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "end_date": self.end_date.isoformat(),
            "failed": self.failed,
            "manifest_path": str(self.manifest_path),
            "mode": self.mode.value,
            "resumed": self.resumed,
            "run_id": str(self.run_id),
            "skipped": self.skipped,
            "start_date": self.start_date.isoformat(),
            "started_at": self.started_at.isoformat(),
            "succeeded": self.succeeded,
            "successful": self.successful,
        }


class BaseballSavantBackfillCoordinator:
    """Backfill an inclusive date range without touching daily state.

    This is intentionally a local/operator-driven workflow. It shares Savant's
    raw landing contract, but owns a separate resumable manifest and never
    advances the scheduled acquisition watermark.
    """

    def __init__(
        self,
        api: BaseballSavantApi,
        raw_store: BaseballSavantRawStore,
        backfill_store: BaseballSavantBackfillStore,
        clock: Clock = utc_now,
        run_id_factory: RunIdFactory = uuid4,
        sleeper: Sleeper = time.sleep,
    ) -> None:
        self.api = api
        self.raw_store = raw_store
        self.backfill_store = backfill_store
        self.clock = clock
        self.run_id_factory = run_id_factory
        self.sleeper = sleeper

    def run(
        self,
        *,
        start_date: date,
        end_date: date,
        mode: BaseballSavantBackfillMode = BaseballSavantBackfillMode.MISSING,
        dry_run: bool = False,
        request_delay_seconds: float = 0.5,
        run_id: Optional[UUID] = None,
        started_at: Optional[datetime] = None,
    ) -> BaseballSavantBackfillResult:
        """Acquire or plan every date in one bounded historical interval."""

        self._validate_options(start_date, end_date, request_delay_seconds)
        resolved_run_id = run_id or self.run_id_factory()
        resolved_started_at = as_utc(started_at or self.clock(), "started_at")
        if end_date >= resolved_started_at.date():
            raise ValueError("Savant backfill end_date must be before today in UTC")
        planned_dates = self._inclusive_dates(start_date, end_date)
        started = self.backfill_store.start(
            run_id=resolved_run_id,
            started_at=resolved_started_at,
            start_date=start_date,
            end_date=end_date,
            mode=mode.value,
            dry_run=dry_run,
            configuration={
                "request_delay_seconds": request_delay_seconds,
                "request_shape": "all-players-one-date",
            },
        )
        LOGGER.info(
            "Savant backfill started run_id=%s started_at=%s dates=%s..%s "
            "mode=%s resumed=%s dry_run=%s",
            resolved_run_id,
            resolved_started_at.isoformat(),
            start_date,
            end_date,
            mode.value,
            started.resumed,
            dry_run,
        )
        requested_dates = [
            game_date
            for game_date in planned_dates
            # A resumed run retries failed/pending dates while preserving work
            # that already reached a terminal outcome.
            if started.date_statuses[game_date] not in {"succeeded", "skipped"}
        ]
        for index, game_date in enumerate(requested_dates):
            made_request = self._process_date(
                game_date=game_date,
                mode=mode,
                dry_run=dry_run,
                run_id=resolved_run_id,
                manifest_path=started.manifest_path,
            )
            if (
                made_request
                and request_delay_seconds > 0
                and index < len(requested_dates) - 1
            ):
                self.sleeper(request_delay_seconds)

        counts = self.backfill_store.finalize(started.manifest_path)
        LOGGER.info(
            "Savant backfill finished run_id=%s succeeded=%d skipped=%d failed=%d",
            resolved_run_id,
            counts["succeeded"],
            counts["skipped"],
            counts["failed"],
        )
        return BaseballSavantBackfillResult(
            run_id=resolved_run_id,
            started_at=resolved_started_at,
            start_date=start_date,
            end_date=end_date,
            manifest_path=started.manifest_path,
            mode=mode,
            dry_run=dry_run,
            resumed=started.resumed,
            succeeded=counts["succeeded"],
            skipped=counts["skipped"],
            failed=counts["failed"],
        )

    def _process_date(
        self,
        *,
        game_date: date,
        mode: BaseballSavantBackfillMode,
        dry_run: bool,
        run_id: UUID,
        manifest_path: ArtifactReference,
    ) -> bool:
        try:
            existing_revision_id = self.raw_store.current_revision_id(game_date)
        except (BaseballSavantStorageError, OSError) as exc:
            LOGGER.exception(
                "Savant backfill current revision failed game_date=%s", game_date
            )
            self.backfill_store.record_date(
                manifest_path,
                game_date,
                "failed",
                {
                    "error_message": str(exc)[:500],
                    "error_type": type(exc).__name__,
                },
            )
            return False
        if dry_run:
            self.backfill_store.record_date(
                manifest_path,
                game_date,
                "skipped",
                {
                    "existing_revision_id": existing_revision_id,
                    "reason": "dry_run",
                    "would_request": (
                        mode == BaseballSavantBackfillMode.VERIFY
                        or existing_revision_id is None
                    ),
                },
            )
            return False
        if (
            mode == BaseballSavantBackfillMode.MISSING
            and existing_revision_id is not None
        ):
            self.backfill_store.record_date(
                manifest_path,
                game_date,
                "skipped",
                {
                    "existing_revision_id": existing_revision_id,
                    "reason": "current_revision_exists",
                },
            )
            return False
        try:
            landed = acquire_statcast_date(
                self.api,
                self.raw_store,
                game_date,
                run_id,
            )
        except (
            BaseballSavantContractError,
            BaseballSavantError,
            BaseballSavantStorageError,
            OSError,
        ) as exc:
            LOGGER.exception("Savant backfill date failed game_date=%s", game_date)
            self.backfill_store.record_date(
                manifest_path,
                game_date,
                "failed",
                {
                    "error_message": str(exc)[:500],
                    "error_type": type(exc).__name__,
                },
            )
            return True
        self.backfill_store.record_date(
            manifest_path,
            game_date,
            "succeeded",
            landed.as_dict(),
        )
        return True

    @staticmethod
    def _inclusive_dates(start_date: date, end_date: date) -> Tuple[date, ...]:
        return tuple(
            date.fromordinal(ordinal)
            for ordinal in range(start_date.toordinal(), end_date.toordinal() + 1)
        )

    @staticmethod
    def _validate_options(
        start_date: date,
        end_date: date,
        request_delay_seconds: float,
    ) -> None:
        if type(start_date) is not date or type(end_date) is not date:
            raise ValueError("start_date and end_date must be dates")
        if start_date > end_date:
            raise ValueError("start_date must not be after end_date")
        if request_delay_seconds < 0:
            raise ValueError("request_delay_seconds must not be negative")
