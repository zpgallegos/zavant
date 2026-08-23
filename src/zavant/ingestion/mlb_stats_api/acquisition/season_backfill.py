"""Resumable historical-season acquisition and reconciliation."""

from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import Any, Callable, Dict, List, Optional, Protocol, Set, Tuple
from uuid import UUID, uuid4

from zavant._time import Clock, as_utc, utc_now
from zavant.ingestion.mlb_stats_api.acquisition.backfill_corrections import SeasonCorrectionDiscoverer
from zavant.ingestion.mlb_stats_api.acquisition.backfill_modes import SeasonBackfillMode
from zavant.ingestion.mlb_stats_api.acquisition.backfill_month import BackfillMonthProcessor
from zavant.ingestion.mlb_stats_api.acquisition.game_changes import GameChangesPollingError
from zavant.ingestion.mlb_stats_api.acquisition.game_eligibility import (
    FinalRegularSeasonGamePolicy,
    GameEligibilityPolicy,
)
from zavant.ingestion.mlb_stats_api.acquisition.protocols import (
    GameChangesApi,
    ScheduleAndLiveGameApi,
)
from zavant.ingestion.mlb_stats_api.client import MlbStatsApiError
from zavant.ingestion.mlb_stats_api.contracts.raw_game import RawGameContractError
from zavant.storage.artifacts import ArtifactReference
from zavant.ingestion.mlb_stats_api.storage.errors import RawGameConflictError, ScheduleConflictError
from zavant.ingestion.mlb_stats_api.storage.models import SeasonBackfillCheckpoint
from zavant.ingestion.mlb_stats_api.storage.protocols import RawGameStore, ScheduleStore, SeasonBackfillStore


RunIdFactory = Callable[[], UUID]
LOGGER = logging.getLogger(__name__)


class MlbSeasonBackfillApi(
    ScheduleAndLiveGameApi,
    GameChangesApi,
    Protocol,
):
    """Complete MLB client surface needed by historical reconciliation."""


@dataclass(frozen=True)
class SeasonBackfillResult:
    """Durable result of reconciling one or more historical seasons."""

    run_id: UUID
    started_at: datetime
    manifest_path: ArtifactReference
    mode: SeasonBackfillMode
    dry_run: bool
    resumed: bool
    season_statuses: Dict[int, str]

    @property
    def successful(self) -> bool:
        return bool(self.season_statuses) and all(
            status == "complete" for status in self.season_statuses.values()
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "manifest_path": str(self.manifest_path),
            "mode": self.mode.value,
            "resumed": self.resumed,
            "run_id": str(self.run_id),
            "season_statuses": {
                str(season): status
                for season, status in sorted(self.season_statuses.items())
            },
            "started_at": self.started_at.isoformat(),
            "successful": self.successful,
        }


class SeasonBackfillCoordinator:
    """Plan monthly schedules and reconcile eligible historical games."""

    def __init__(
        self,
        api: MlbSeasonBackfillApi,
        schedule_store: ScheduleStore,
        game_store: RawGameStore,
        backfill_store: SeasonBackfillStore,
        eligibility_policy: Optional[GameEligibilityPolicy] = None,
        clock: Clock = utc_now,
        run_id_factory: RunIdFactory = uuid4,
    ) -> None:
        resolved_policy = (
            eligibility_policy
            if eligibility_policy is not None
            else FinalRegularSeasonGamePolicy()
        )
        self.game_store = game_store
        self.backfill_store = backfill_store
        self.clock = clock
        self.run_id_factory = run_id_factory
        self.month_processor = BackfillMonthProcessor(
            api, schedule_store, game_store, resolved_policy
        )
        self.correction_discoverer = SeasonCorrectionDiscoverer(
            api, backfill_store
        )
        self.eligibility_policy = resolved_policy

    def run(
        self,
        seasons: Tuple[int, ...],
        mode: SeasonBackfillMode = SeasonBackfillMode.RECONCILE,
        dry_run: bool = False,
        sport_id: int = 1,
        correction_limit: int = 1000,
        correction_overlap: timedelta = timedelta(minutes=5),
        correction_max_pages: int = 100,
        run_id: Optional[UUID] = None,
        started_at: Optional[datetime] = None,
    ) -> SeasonBackfillResult:
        normalized_seasons = self._validate_options(
            seasons,
            sport_id,
            correction_limit,
            correction_overlap,
            correction_max_pages,
        )
        resolved_run_id = run_id or self.run_id_factory()
        resolved_started_at = as_utc(started_at or self.clock(), "started_at")
        future_seasons = tuple(
            season for season in normalized_seasons if season > resolved_started_at.year
        )
        if future_seasons:
            raise ValueError(
                f"future backfill seasons are not allowed: {future_seasons}"
            )
        started = self.backfill_store.start(
            run_id=resolved_run_id,
            started_at=resolved_started_at,
            seasons=normalized_seasons,
            mode=mode.value,
            dry_run=dry_run,
            configuration={
                "correction_limit": correction_limit,
                "correction_max_pages": correction_max_pages,
                "correction_overlap_seconds": correction_overlap.total_seconds(),
                "eligibility_policy": type(self.eligibility_policy).__name__,
                "sport_id": sport_id,
            },
        )
        LOGGER.info(
            "backfill run started run_id=%s seasons=%s mode=%s resumed=%s dry_run=%s",
            resolved_run_id,
            normalized_seasons,
            mode.value,
            started.resumed,
            dry_run,
        )
        for season in normalized_seasons:
            if started.season_statuses.get(season) == "complete":
                LOGGER.info("backfill season already complete season=%s", season)
                continue
            LOGGER.info("backfill season started season=%s", season)
            try:
                details = self._run_season(
                    season=season,
                    mode=mode,
                    dry_run=dry_run,
                    sport_id=sport_id,
                    correction_limit=correction_limit,
                    correction_overlap=correction_overlap,
                    correction_max_pages=correction_max_pages,
                    run_id=resolved_run_id,
                    started_at=resolved_started_at,
                    manifest_path=started.manifest_path,
                )
            except (
                GameChangesPollingError,
                MlbStatsApiError,
                OSError,
                RawGameConflictError,
                RawGameContractError,
                ScheduleConflictError,
                ValueError,
            ) as exc:
                self.backfill_store.record_season(
                    started.manifest_path,
                    season,
                    "failed",
                    {
                        "error_message": str(exc)[:500],
                        "error_type": type(exc).__name__,
                    },
                )
                LOGGER.exception("backfill season failed season=%s", season)
                continue
            season_status = "complete" if details["season_complete"] else "failed"
            self.backfill_store.record_season(
                started.manifest_path,
                season,
                season_status,
                details,
            )
            LOGGER.info(
                "backfill season finished season=%s status=%s downloaded=%s "
                "revisions_created=%s unchanged=%s",
                season,
                season_status,
                details["downloaded"],
                details["revisions_created"],
                details["unchanged"],
            )
        statuses = self.backfill_store.finalize(started.manifest_path)
        return SeasonBackfillResult(
            run_id=resolved_run_id,
            started_at=resolved_started_at,
            manifest_path=started.manifest_path,
            mode=mode,
            dry_run=dry_run,
            resumed=started.resumed,
            season_statuses=statuses,
        )

    def _run_season(
        self,
        season: int,
        mode: SeasonBackfillMode,
        dry_run: bool,
        sport_id: int,
        correction_limit: int,
        correction_overlap: timedelta,
        correction_max_pages: int,
        run_id: UUID,
        started_at: datetime,
        manifest_path: ArtifactReference,
    ) -> Dict[str, Any]:
        current_revisions = self.game_store.current_revisions(season)
        existing_game_pks = {
            revision.game_pk for revision in current_revisions
        }
        checkpoint = self.backfill_store.read_checkpoint(season)
        checkpoint_committed = self._checkpoint_committed_by_run(
            checkpoint,
            run_id,
            started_at,
            manifest_path,
        )
        changed_game_pks = set()
        correction_paths: Tuple[ArtifactReference, ...] = ()
        if (
            mode == SeasonBackfillMode.RECONCILE
            and current_revisions
            and not checkpoint_committed
        ):
            lower_checkpoint = (
                checkpoint.updated_since
                if checkpoint is not None
                else min(revision.observed_at for revision in current_revisions)
            )
            correction = self.correction_discoverer.discover(
                season=season,
                updated_since=lower_checkpoint - correction_overlap,
                window_end=started_at,
                sport_id=sport_id,
                limit=correction_limit,
                max_pages=correction_max_pages,
                run_id=run_id,
            )
            changed_game_pks = correction.game_pks
            correction_paths = correction.response_paths

        counters = {
            "correction_candidates": 0,
            "deferred": 0,
            "downloaded": 0,
            "eligible": 0,
            "existing_before": len(existing_game_pks),
            "failed": 0,
            "missing_before": 0,
            "revisions_created": 0,
            "schedule_entries": 0,
            "scheduled": 0,
            "selected": 0,
            "skipped": 0,
            "unchanged": 0,
        }
        schedule_manifests: List[str] = []
        resolved_game_pks: Set[int] = set()
        scheduled_game_pks: Set[int] = set()
        eligible_game_pks: Set[int] = set()
        deferred_game_pks: Set[int] = set()
        for month in range(1, 13):
            LOGGER.info("backfill month started season=%s month=%02d", season, month)
            result = self.month_processor.run(
                season=season,
                month=month,
                mode=mode,
                dry_run=dry_run,
                sport_id=sport_id,
                run_id=run_id,
                requested_at=started_at,
                existing_game_pks=existing_game_pks,
                changed_game_pks=changed_game_pks,
                resolved_game_pks=resolved_game_pks,
            )
            schedule_manifests.append(str(result.manifest_path))
            for name, value in result.counters.items():
                counters[name] += value
            scheduled_game_pks.update(result.scheduled_game_pks)
            eligible_game_pks.update(result.eligible_game_pks)
            deferred_game_pks.update(result.deferred_game_pks)
            LOGGER.info(
                "backfill month finished season=%s month=%02d selected=%s "
                "downloaded=%s failed=%s",
                season,
                month,
                result.counters["selected"],
                result.counters["downloaded"],
                result.counters["failed"],
            )

        unresolved_deferred_game_pks = deferred_game_pks - eligible_game_pks
        season_complete = (
            counters["failed"] == 0
            and bool(scheduled_game_pks)
            and bool(eligible_game_pks)
            and not unresolved_deferred_game_pks
        )
        checkpoint_advanced = (
            season_complete
            and not dry_run
            and mode in {SeasonBackfillMode.RECONCILE, SeasonBackfillMode.VERIFY}
        )
        if checkpoint_advanced and not checkpoint_committed:
            self.backfill_store.advance_checkpoint(
                season=season,
                expected_current=(
                    checkpoint.updated_since if checkpoint is not None else None
                ),
                updated_since=started_at,
                run_id=run_id,
                manifest_path=manifest_path,
            )
        return {
            **counters,
            "checkpoint_recovered": checkpoint_committed,
            "checkpoint_after": started_at.isoformat() if checkpoint_advanced else None,
            "checkpoint_before": (
                checkpoint.updated_since.isoformat()
                if checkpoint is not None
                else None
            ),
            "correction_evidence": [str(path) for path in correction_paths],
            "duplicate_schedule_entries": counters["schedule_entries"]
            - len(scheduled_game_pks),
            "public_correction_scope": "non_metric_game_changes",
            "schedule_manifests": schedule_manifests,
            "season_complete": season_complete,
            "unique_eligible": len(eligible_game_pks),
            "unique_scheduled": len(scheduled_game_pks),
            "unresolved_deferred": len(unresolved_deferred_game_pks),
            "unresolved_deferred_game_pks": sorted(unresolved_deferred_game_pks),
        }

    @staticmethod
    def _checkpoint_committed_by_run(
        checkpoint: Optional[SeasonBackfillCheckpoint],
        run_id: UUID,
        started_at: datetime,
        manifest_path: ArtifactReference,
    ) -> bool:
        return bool(
            checkpoint is not None
            and checkpoint.run_id == run_id
            and checkpoint.updated_since == started_at
            and checkpoint.manifest_path.key == manifest_path.key
        )

    @staticmethod
    def _validate_options(
        seasons: Tuple[int, ...],
        sport_id: int,
        correction_limit: int,
        correction_overlap: timedelta,
        correction_max_pages: int,
    ) -> Tuple[int, ...]:
        if not seasons or any(type(season) is not int or season <= 0 for season in seasons):
            raise ValueError("seasons must contain positive integers")
        if type(sport_id) is not int or sport_id <= 0:
            raise ValueError("sport_id must be a positive integer")
        if type(correction_limit) is not int or correction_limit <= 0:
            raise ValueError("correction_limit must be a positive integer")
        if correction_overlap < timedelta(0):
            raise ValueError("correction_overlap must not be negative")
        if type(correction_max_pages) is not int or correction_max_pages <= 0:
            raise ValueError("correction_max_pages must be a positive integer")
        return tuple(sorted(set(seasons)))
