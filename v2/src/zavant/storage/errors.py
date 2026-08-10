"""Errors shared by acquisition persistence stores."""


class RawGameConflictError(RuntimeError):
    """Raised when a raw-game revision contains inconsistent content."""


class ScheduleConflictError(RuntimeError):
    """Raised when a schedule run conflicts with stored content."""


class GameChangesConflictError(RuntimeError):
    """Raised when a correction poll conflicts with stored content."""


class ScheduleWatermarkConflictError(RuntimeError):
    """Raised when schedule state fails compare-before-write validation."""


class GameChangesWatermarkConflictError(RuntimeError):
    """Raised when correction state fails compare-before-write validation."""


class DailyRunConflictError(RuntimeError):
    """Raised when a daily run manifest is malformed or conflicts."""


class SeasonBackfillConflictError(RuntimeError):
    """Raised when season-backfill evidence or state conflicts."""
