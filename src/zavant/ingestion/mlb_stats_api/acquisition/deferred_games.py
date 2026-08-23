"""Daily reconciliation of games retained while non-final."""

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from zavant.ingestion.mlb_stats_api.acquisition.live_games import GameIdentityError, retrieve_live_game
from zavant.ingestion.mlb_stats_api.acquisition.protocols import LiveGameApi
from zavant.ingestion.mlb_stats_api.client import MlbStatsApiError
from zavant.ingestion.mlb_stats_api.contracts.raw_game import RawGameContractError, RawGameResponse
from zavant.ingestion.mlb_stats_api.storage.errors import DeferredGameConflictError, RawGameConflictError
from zavant.ingestion.mlb_stats_api.storage.protocols import DeferredGameStore, RawGameStore


@dataclass(frozen=True)
class DeferredGameProcessingResult:
    """Outcome counts for one pass over the durable deferred worklist."""

    status: str
    summary: Dict[str, int]
    deferred_game_pks: Tuple[int, ...]
    failed_game_pks: Tuple[int, ...]
    skipped_game_pks: Tuple[int, ...]
    succeeded_game_pks: Tuple[int, ...]

    @property
    def successful(self) -> bool:
        return self.status == "complete"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "deferred_game_pks": list(self.deferred_game_pks),
            "failed_game_pks": list(self.failed_game_pks),
            "skipped_game_pks": list(self.skipped_game_pks),
            "status": self.status,
            "succeeded_game_pks": list(self.succeeded_game_pks),
            "summary": self.summary,
        }


class DeferredGameProcessor:
    """Reconsider every deferred game independently of schedule lookback."""

    def __init__(
        self,
        api: LiveGameApi,
        deferred_game_store: DeferredGameStore,
        game_store: RawGameStore,
    ) -> None:
        self.api = api
        self.deferred_game_store = deferred_game_store
        self.game_store = game_store

    def process_all(self) -> DeferredGameProcessingResult:
        outcomes: Dict[str, List[int]] = {
            "deferred": [],
            "failed": [],
            "skipped": [],
            "succeeded": [],
        }
        for item in self.deferred_game_store.pending():
            try:
                if self.game_store.current_revision_id(
                    season=item.season,
                    game_pk=item.game_pk,
                ) is not None:
                    self.deferred_game_store.resolve(item.game_pk)
                    outcomes["succeeded"].append(item.game_pk)
                    continue
                retrieved, game = retrieve_live_game(
                    self.api, item.game_pk, item.season
                )
                status_code = _status_code(game)
                if status_code == "C":
                    self.deferred_game_store.resolve(item.game_pk)
                    outcomes["skipped"].append(item.game_pk)
                elif status_code != "F":
                    self.deferred_game_store.defer(
                        game_pk=item.game_pk,
                        season=item.season,
                        official_date=game.official_date,
                        live_feed_link=item.live_feed_link,
                    )
                    outcomes["deferred"].append(item.game_pk)
                else:
                    self.game_store.land(
                        game=game,
                        raw=retrieved.body,
                        source_uri=retrieved.source_uri,
                        trigger="deferred_schedule",
                    )
                    self.deferred_game_store.resolve(item.game_pk)
                    outcomes["succeeded"].append(item.game_pk)
            except (
                DeferredGameConflictError,
                GameIdentityError,
                MlbStatsApiError,
                OSError,
                RawGameConflictError,
                RawGameContractError,
                ValueError,
            ):
                outcomes["failed"].append(item.game_pk)
        summary = {status: len(game_pks) for status, game_pks in outcomes.items()}
        return DeferredGameProcessingResult(
            status="failed" if summary["failed"] else "complete",
            summary=summary,
            deferred_game_pks=tuple(outcomes["deferred"]),
            failed_game_pks=tuple(outcomes["failed"]),
            skipped_game_pks=tuple(outcomes["skipped"]),
            succeeded_game_pks=tuple(outcomes["succeeded"]),
        )


def _status_code(game: RawGameResponse) -> str:
    game_data = game.payload.get("gameData")
    status = game_data.get("status") if isinstance(game_data, dict) else None
    status_code = status.get("codedGameState") if isinstance(status, dict) else None
    if not isinstance(status_code, str) or not status_code:
        raise RawGameContractError("gameData.status.codedGameState is required")
    return status_code
