"""Shared retrieval and source-identity validation for live games."""

from typing import Optional, Tuple

from zavant.ingestion.mlb_stats_api.acquisition.protocols import LiveGameApi
from zavant.ingestion.mlb_stats_api.client import RetrievedResource
from zavant.ingestion.mlb_stats_api.contracts.raw_game import RawGameResponse


class GameIdentityError(ValueError):
    """Raised when a live-game response has an unexpected identity."""


def retrieve_live_game(
    api: LiveGameApi,
    game_pk: int,
    expected_season: Optional[int] = None,
) -> Tuple[RetrievedResource, RawGameResponse]:
    """Retrieve, parse, and validate a live game against its requested identity."""
    retrieved = api.get_live_game(game_pk)
    game = RawGameResponse.from_bytes(retrieved.body)
    if game.game_pk != game_pk or (
        expected_season is not None and game.season != expected_season
    ):
        expected = f"gamePk {game_pk}"
        received = f"gamePk {game.game_pk}"
        if expected_season is not None:
            expected = f"season {expected_season} {expected}"
            received = f"season {game.season} {received}"
        raise GameIdentityError(f"expected {expected}, received {received}")
    return retrieved, game
