"""Explicit game-eligibility policy for initial raw-game acquisition."""

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from zavant.contracts.schedule import ScheduledGame


class EligibilityDisposition(str, Enum):
    """Possible acquisition decisions for a discovered schedule game."""

    ELIGIBLE = "eligible"
    DEFERRED = "deferred"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class GameEligibilityDecision:
    """Decision produced by an initial-acquisition policy.

    Attributes:
        disposition: Whether to acquire, defer, or permanently skip the game.
        reason: Stable machine-readable explanation for the decision.
    """

    disposition: EligibilityDisposition
    reason: str


class GameEligibilityPolicy(Protocol):
    """Policy boundary for deciding how to handle a scheduled game."""

    def evaluate(self, game: ScheduledGame) -> GameEligibilityDecision:
        """Evaluate one scheduled game.

        Args:
            game: Validated game discovered in a schedule response.

        Returns:
            Acquisition disposition and stable reason.
        """

        ...


class FinalRegularSeasonGamePolicy:
    """Acquire finalized regular-season games and classify all others."""

    def evaluate(self, game: ScheduledGame) -> GameEligibilityDecision:
        """Classify a scheduled game for the initial acquisition workflow.

        Args:
            game: Validated game discovered in a schedule response.

        Returns:
            Eligible for final regular-season games, deferred for unfinished
            regular-season games, or skipped for other game types.
        """

        if game.game_type != "R":
            return GameEligibilityDecision(
                disposition=EligibilityDisposition.SKIPPED,
                reason="unsupported_game_type",
            )
        if game.status_code != "F":
            return GameEligibilityDecision(
                disposition=EligibilityDisposition.DEFERRED,
                reason="game_not_final",
            )
        return GameEligibilityDecision(
            disposition=EligibilityDisposition.ELIGIBLE,
            reason="final_regular_season_game",
        )
