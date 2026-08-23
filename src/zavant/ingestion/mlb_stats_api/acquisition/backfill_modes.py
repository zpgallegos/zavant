"""Historical game-selection modes."""

from enum import Enum


class SeasonBackfillMode(str, Enum):
    """How existing raw games participate in a historical run."""

    MISSING = "missing"
    RECONCILE = "reconcile"
    VERIFY = "verify"
