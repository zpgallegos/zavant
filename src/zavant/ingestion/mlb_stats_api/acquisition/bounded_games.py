"""Bounded schedule-to-game acquisition workflow."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, Optional
from uuid import UUID, uuid4

from zavant._time import Clock, as_utc, utc_now
from zavant.ingestion.mlb_stats_api.acquisition.game_eligibility import (
    EligibilityDisposition,
    FinalRegularSeasonGamePolicy,
    GameEligibilityPolicy,
)
from zavant.ingestion.mlb_stats_api.acquisition.live_games import GameIdentityError, retrieve_live_game
from zavant.ingestion.mlb_stats_api.acquisition.protocols import ScheduleAndLiveGameApi
from zavant.ingestion.mlb_stats_api.client import MlbStatsApiError
from zavant.ingestion.mlb_stats_api.contracts.raw_game import RawGameContractError
from zavant.ingestion.mlb_stats_api.contracts.schedule import (
    ScheduleRequest,
    ScheduleResponse,
)
from zavant.storage.artifacts import ArtifactReference
from zavant.ingestion.mlb_stats_api.storage.errors import RawGameConflictError, ScheduleConflictError
from zavant.ingestion.mlb_stats_api.storage.protocols import DeferredGameStore, RawGameStore, ScheduleStore


RunIdFactory = Callable[[], UUID]


@dataclass(frozen=True)
class BoundedGameAcquisitionResult:
    """Summary of one bounded schedule-to-game acquisition run."""

    run_id: UUID
    requested_at: datetime
    manifest_path: ArtifactReference
    status: str
    summary: Dict[str, int]
    schedule_created: bool
    resumed: bool
    schedule_http_attempts: int

    @property
    def successful(self) -> bool:
        return self.status == "complete"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "manifest_path": str(self.manifest_path),
            "requested_at": self.requested_at.isoformat(),
            "resumed": self.resumed,
            "run_id": str(self.run_id),
            "schedule_created": self.schedule_created,
            "schedule_http_attempts": self.schedule_http_attempts,
            "status": self.status,
            "summary": self.summary,
        }


class BoundedGameAcquirer:
    """Coordinate schedule discovery and initial raw-game acquisition.

    This is the reusable schedule-to-game building block underneath both
    incremental discovery and explicit bounded acquisition. Its persisted
    schedule response is also the stable work list used to resume a run.

    Args:
        api: MLB client supporting schedule and live-game retrieval.
        schedule_store: Schedule evidence and manifest store.
        game_store: Revision-aware raw-game store.
        eligibility_policy: Explicit policy classifying scheduled games.
        clock: Function returning the current timezone-aware UTC time.
        run_id_factory: Function generating a new run identifier.
    """

    def __init__(
        self,
        api: ScheduleAndLiveGameApi,
        schedule_store: ScheduleStore,
        game_store: RawGameStore,
        eligibility_policy: Optional[GameEligibilityPolicy] = None,
        deferred_game_store: Optional[DeferredGameStore] = None,
        clock: Clock = utc_now,
        run_id_factory: RunIdFactory = uuid4,
    ) -> None:
        self.api = api
        self.schedule_store = schedule_store
        self.game_store = game_store
        self.eligibility_policy = (
            eligibility_policy
            if eligibility_policy is not None
            else FinalRegularSeasonGamePolicy()
        )
        self.deferred_game_store = deferred_game_store
        self.clock = clock
        self.run_id_factory = run_id_factory

    def acquire(
        self,
        start_date: date,
        end_date: date,
        sport_id: int = 1,
        run_id: Optional[UUID] = None,
        requested_at: Optional[datetime] = None,
    ) -> BoundedGameAcquisitionResult:
        """Acquire eligible games discovered in a bounded schedule.

        Existing completed, skipped, or deferred manifest entries are not
        repeated. Failed entries are retried when the same run is resumed.

        Args:
            start_date: Inclusive first official date requested.
            end_date: Inclusive last official date requested.
            sport_id: MLB sport identifier, with `1` representing MLB.
            run_id: Optional stable run identifier for deterministic resumption.
            requested_at: Optional original request time for deterministic
                resumption. Defaults to the service clock for new runs.

        Returns:
            Durable run identity, status, manifest path, and outcome counts.

        Raises:
            ValueError: If request boundaries or timestamps are invalid.
            MlbStatsApiError: If a new schedule cannot be retrieved.
            ScheduleContractError: If stored or retrieved schedule bytes fail
                validation.
            ScheduleConflictError: If existing run evidence conflicts with the
                requested run.
            OSError: If schedule evidence or manifest state cannot be stored.
        """

        if start_date > end_date:
            raise ValueError("start_date must not be after end_date")
        if type(sport_id) is not int or sport_id <= 0:
            raise ValueError("sport_id must be a positive integer")

        resolved_run_id = run_id or self.run_id_factory()
        resolved_requested_at = requested_at or self.clock()
        resolved_requested_at = as_utc(resolved_requested_at, "requested_at")

        loaded_run = self.schedule_store.load_run(
            requested_at=resolved_requested_at,
            run_id=resolved_run_id,
        )
        if loaded_run is None:
            retrieved_schedule = self.api.get_schedule(
                start_date=start_date,
                end_date=end_date,
                sport_id=sport_id,
            )
            schedule = ScheduleResponse.from_bytes(retrieved_schedule.body)
            schedule_request = ScheduleRequest(
                start_date=start_date,
                end_date=end_date,
                sport_id=sport_id,
                requested_at=resolved_requested_at,
                source_uri=retrieved_schedule.source_uri,
            )
            landed_schedule = self.schedule_store.land(
                schedule=schedule,
                request=schedule_request,
                raw=retrieved_schedule.body,
                run_id=resolved_run_id,
            )
            manifest_path = landed_schedule.manifest_path
            schedule_created = landed_schedule.created
            resumed = False
            schedule_http_attempts = retrieved_schedule.attempts
        else:
            # Resume from the immutable discovery snapshot. Re-querying the
            # mutable schedule here could change the membership of this run.
            self._validate_resumed_request(
                stored_request=loaded_run.request,
                start_date=start_date,
                end_date=end_date,
                sport_id=sport_id,
                requested_at=resolved_requested_at,
            )
            schedule = ScheduleResponse.from_bytes(loaded_run.raw)
            manifest_path = loaded_run.manifest_path
            schedule_created = False
            resumed = True
            schedule_http_attempts = 0

        existing_statuses = self.schedule_store.game_statuses(manifest_path)
        if set(existing_statuses) != set(schedule.game_pks):
            raise ScheduleConflictError(
                "schedule manifest games conflict with the stored response"
            )
        terminal_statuses = {"deferred", "skipped", "succeeded"}
        for scheduled_game in schedule.scheduled_games:
            current_status = existing_statuses[scheduled_game.game_pk]
            # Failed and pending entries remain retryable; terminal entries
            # make resumption idempotent.
            if current_status in terminal_statuses:
                continue

            decision = self.eligibility_policy.evaluate(scheduled_game)
            if decision.disposition == EligibilityDisposition.SKIPPED:
                self.schedule_store.record_game_outcome(
                    manifest_path=manifest_path,
                    game_pk=scheduled_game.game_pk,
                    status="skipped",
                    details={"reason": decision.reason},
                )
                self._resolve_deferred(scheduled_game.game_pk)
                continue
            if decision.disposition == EligibilityDisposition.DEFERRED:
                self.schedule_store.record_game_outcome(
                    manifest_path=manifest_path,
                    game_pk=scheduled_game.game_pk,
                    status="deferred",
                    details={"reason": decision.reason},
                )
                if self.deferred_game_store is not None:
                    self.deferred_game_store.defer(
                        game_pk=scheduled_game.game_pk,
                        season=scheduled_game.season,
                        official_date=scheduled_game.official_date,
                        live_feed_link=scheduled_game.live_feed_link,
                    )
                continue

            acquired = self._acquire_game(
                manifest_path=manifest_path,
                game_pk=scheduled_game.game_pk,
                season=scheduled_game.season,
            )
            if acquired:
                self._resolve_deferred(scheduled_game.game_pk)

        summary = self.schedule_store.finalize_manifest(manifest_path)
        if summary["pending"]:
            status = "incomplete"
        elif summary["failed"]:
            status = "failed"
        else:
            status = "complete"
        return BoundedGameAcquisitionResult(
            run_id=resolved_run_id,
            requested_at=resolved_requested_at,
            manifest_path=manifest_path,
            status=status,
            summary=summary,
            schedule_created=schedule_created,
            resumed=resumed,
            schedule_http_attempts=schedule_http_attempts,
        )

    def _acquire_game(
        self, manifest_path: ArtifactReference, game_pk: int, season: int
    ) -> bool:
        try:
            current_revision_id = self.game_store.current_revision_id(
                season=season,
                game_pk=game_pk,
            )
            if current_revision_id is not None:
                # Another run may already have landed the same game. Resolve
                # this manifest from durable raw state without another request.
                self.schedule_store.record_game_outcome(
                    manifest_path=manifest_path,
                    game_pk=game_pk,
                    status="succeeded",
                    details={
                        "reason": "game_already_landed",
                        "revision_created": False,
                        "revision_id": current_revision_id,
                    },
                )
                return True
            retrieved_game, game = retrieve_live_game(self.api, game_pk, season)
            landed_game = self.game_store.land(
                game=game,
                raw=retrieved_game.body,
                source_uri=retrieved_game.source_uri,
                trigger="initial",
            )
        except (
            GameIdentityError,
            MlbStatsApiError,
            OSError,
            RawGameConflictError,
            RawGameContractError,
            ValueError,
        ) as exc:
            self.schedule_store.record_game_outcome(
                manifest_path=manifest_path,
                game_pk=game_pk,
                status="failed",
                details={
                    "error_message": str(exc)[:500],
                    "error_type": type(exc).__name__,
                },
            )
            return False

        self.schedule_store.record_game_outcome(
            manifest_path=manifest_path,
            game_pk=game_pk,
            status="succeeded",
            details={
                "http_attempts": retrieved_game.attempts,
                "revision_created": landed_game.created,
                "revision_id": landed_game.revision_id,
                "source_uri": retrieved_game.source_uri,
            },
        )
        return True

    def _resolve_deferred(self, game_pk: int) -> None:
        if self.deferred_game_store is not None:
            self.deferred_game_store.resolve(game_pk)

    @staticmethod
    def _validate_resumed_request(
        stored_request: Dict[str, Any],
        start_date: date,
        end_date: date,
        sport_id: int,
        requested_at: datetime,
    ) -> None:
        expected = {
            "end_date": end_date.isoformat(),
            "requested_at": requested_at.isoformat(),
            "sport_id": sport_id,
            "start_date": start_date.isoformat(),
        }
        for key, value in expected.items():
            if stored_request.get(key) != value:
                raise ScheduleConflictError(
                    f"stored schedule request has a conflicting {key}"
                )
