"""One bounded monthly unit of historical game acquisition."""

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, FrozenSet, Optional, Protocol, Set
from uuid import UUID, uuid5

from zavant.acquisition.backfill_modes import SeasonBackfillMode
from zavant.acquisition.bounded_games import GameIdentityError
from zavant.acquisition.game_eligibility import (
    EligibilityDisposition,
    GameEligibilityPolicy,
)
from zavant.clients.mlb_stats_api import MlbStatsApiError, RetrievedResource
from zavant.contracts.raw_game import RawGameContractError, RawGameResponse
from zavant.contracts.schedule import ScheduleRequest, ScheduleResponse
from zavant.storage.artifacts import ArtifactReference
from zavant.storage.errors import RawGameConflictError
from zavant.storage.protocols import RawGameStore, ScheduleStore


class MlbBackfillGamesApi(Protocol):
    def get_schedule(
        self, start_date: date, end_date: date, sport_id: int = 1
    ) -> RetrievedResource:
        ...

    def get_live_game(self, game_pk: int) -> RetrievedResource:
        ...


@dataclass(frozen=True)
class BackfillMonthResult:
    manifest_path: ArtifactReference
    counters: Dict[str, int]
    scheduled_game_pks: FrozenSet[int]
    eligible_game_pks: FrozenSet[int]
    deferred_game_pks: FrozenSet[int]


class BackfillMonthProcessor:
    """Land a monthly schedule and execute its selected game work."""

    def __init__(
        self,
        api: MlbBackfillGamesApi,
        schedule_store: ScheduleStore,
        game_store: RawGameStore,
        eligibility_policy: GameEligibilityPolicy,
    ) -> None:
        self.api = api
        self.schedule_store = schedule_store
        self.game_store = game_store
        self.eligibility_policy = eligibility_policy

    def run(
        self,
        season: int,
        month: int,
        mode: SeasonBackfillMode,
        dry_run: bool,
        sport_id: int,
        run_id: UUID,
        requested_at: datetime,
        existing_game_pks: Set[int],
        changed_game_pks: Set[int],
        resolved_game_pks: Set[int],
    ) -> BackfillMonthResult:
        start_date = date(season, month, 1)
        end_date = date(season, month, monthrange(season, month)[1])
        month_run_id = uuid5(run_id, f"season={season}/month={month:02d}")
        loaded = self.schedule_store.load_run(requested_at, month_run_id)
        if loaded is None:
            retrieved = self.api.get_schedule(start_date, end_date, sport_id)
            schedule = ScheduleResponse.from_bytes(retrieved.body)
            landed = self.schedule_store.land(
                schedule=schedule,
                request=ScheduleRequest(
                    start_date=start_date,
                    end_date=end_date,
                    sport_id=sport_id,
                    requested_at=requested_at,
                    source_uri=retrieved.source_uri,
                ),
                raw=retrieved.body,
                run_id=month_run_id,
            )
            manifest_path = landed.manifest_path
        else:
            schedule = ScheduleResponse.from_bytes(loaded.raw)
            manifest_path = loaded.manifest_path

        statuses = self.schedule_store.game_statuses(manifest_path)
        scheduled_game_pks = {game.game_pk for game in schedule.scheduled_games}
        eligible_game_pks: Set[int] = set()
        deferred_game_pks: Set[int] = set()
        counters = {
            "correction_candidates": 0,
            "deferred": 0,
            "downloaded": 0,
            "eligible": 0,
            "failed": 0,
            "missing_before": 0,
            "revisions_created": 0,
            "schedule_entries": schedule.total_games,
            "scheduled": len(schedule.scheduled_games),
            "selected": 0,
            "skipped": 0,
            "unchanged": 0,
        }
        terminal = {"deferred", "skipped", "succeeded"}
        for game in schedule.scheduled_games:
            if game.season != season:
                counters["skipped"] += 1
                if statuses[game.game_pk] not in terminal:
                    self.schedule_store.record_game_outcome(
                        manifest_path,
                        game.game_pk,
                        "skipped",
                        {"reason": "season_mismatch"},
                    )
                continue
            decision = self.eligibility_policy.evaluate(game)
            if decision.disposition == EligibilityDisposition.SKIPPED:
                counters["skipped"] += 1
                if statuses[game.game_pk] not in terminal:
                    self.schedule_store.record_game_outcome(
                        manifest_path,
                        game.game_pk,
                        "skipped",
                        {"reason": decision.reason},
                    )
                continue
            if decision.disposition == EligibilityDisposition.DEFERRED:
                deferred_game_pks.add(game.game_pk)
                counters["deferred"] += 1
                if statuses[game.game_pk] not in terminal:
                    self.schedule_store.record_game_outcome(
                        manifest_path,
                        game.game_pk,
                        "deferred",
                        {"reason": decision.reason},
                    )
                continue

            eligible_game_pks.add(game.game_pk)
            if game.game_pk in resolved_game_pks:
                if statuses[game.game_pk] not in terminal:
                    self.schedule_store.record_game_outcome(
                        manifest_path,
                        game.game_pk,
                        "succeeded",
                        {"reason": "game_resolved_in_prior_month"},
                    )
                continue
            counters["eligible"] += 1
            is_missing = game.game_pk not in existing_game_pks
            is_correction = (
                mode == SeasonBackfillMode.RECONCILE
                and game.game_pk in changed_game_pks
                and not is_missing
            )
            selected = (
                is_missing or mode == SeasonBackfillMode.VERIFY or is_correction
            )
            counters["missing_before"] += int(is_missing)
            counters["correction_candidates"] += int(is_correction)
            counters["selected"] += int(selected)
            if statuses[game.game_pk] in terminal:
                if statuses[game.game_pk] in {"skipped", "succeeded"}:
                    resolved_game_pks.add(game.game_pk)
                continue
            if not selected:
                self.schedule_store.record_game_outcome(
                    manifest_path,
                    game.game_pk,
                    "succeeded",
                    {
                        "reason": "game_already_landed",
                        "revision_created": False,
                        "revision_id": self.game_store.current_revision_id(
                            season, game.game_pk
                        ),
                    },
                )
                resolved_game_pks.add(game.game_pk)
                continue
            reason = (
                "missing"
                if is_missing
                else "public_correction"
                if is_correction
                else "full_verification"
            )
            if dry_run:
                self.schedule_store.record_game_outcome(
                    manifest_path,
                    game.game_pk,
                    "skipped",
                    {"reason": f"dry_run_would_download_{reason}"},
                )
                resolved_game_pks.add(game.game_pk)
                continue
            created = self._download(
                manifest_path, game.game_pk, season, reason, mode
            )
            if created is None:
                counters["failed"] += 1
            else:
                resolved_game_pks.add(game.game_pk)
                counters["downloaded"] += 1
                counters["revisions_created"] += int(created)
                counters["unchanged"] += int(not created)

        counters["failed"] = self.schedule_store.finalize_manifest(manifest_path)[
            "failed"
        ]
        return BackfillMonthResult(
            manifest_path=manifest_path,
            counters=counters,
            scheduled_game_pks=frozenset(scheduled_game_pks),
            eligible_game_pks=frozenset(eligible_game_pks),
            deferred_game_pks=frozenset(deferred_game_pks),
        )

    def _download(
        self,
        manifest_path: ArtifactReference,
        game_pk: int,
        season: int,
        reason: str,
        mode: SeasonBackfillMode,
    ) -> Optional[bool]:
        try:
            retrieved = self.api.get_live_game(game_pk)
            game = RawGameResponse.from_bytes(retrieved.body)
            if game.game_pk != game_pk or game.season != season:
                raise GameIdentityError(
                    f"expected season {season} gamePk {game_pk}, received "
                    f"season {game.season} gamePk {game.game_pk}"
                )
            landed = self.game_store.land(
                game=game,
                raw=retrieved.body,
                source_uri=retrieved.source_uri,
                trigger=f"backfill_{mode.value}",
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
                manifest_path,
                game_pk,
                "failed",
                {
                    "error_message": str(exc)[:500],
                    "error_type": type(exc).__name__,
                    "reason": reason,
                },
            )
            return None
        self.schedule_store.record_game_outcome(
            manifest_path,
            game_pk,
            "succeeded",
            {
                "http_attempts": retrieved.attempts,
                "reason": reason,
                "revision_created": landed.created,
                "revision_id": landed.revision_id,
                "source_uri": retrieved.source_uri,
            },
        )
        return landed.created
