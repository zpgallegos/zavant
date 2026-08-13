"""Values exchanged by analytical projection components."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Tuple

from zavant.contracts.raw_game import RawGameResponse
from zavant.projection.contracts import ProjectionRow


@dataclass(frozen=True)
class ProjectionSource:
    """A validated raw game plus immutable revision provenance."""

    game: RawGameResponse
    revision_id: str
    observed_at: datetime
    source_uri: str
    raw_object_uri: str

    def __post_init__(self) -> None:
        if not self.revision_id:
            raise ValueError("revision_id must not be empty")
        if self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if not self.raw_object_uri:
            raise ValueError("raw_object_uri must not be empty")


@dataclass(frozen=True)
class GameProjection:
    """All analytical rows produced from one game revision."""

    table_rows: Dict[str, Tuple[ProjectionRow, ...]]
    event_kind_counts: Dict[str, int]

    def tables(self) -> Dict[str, Tuple[ProjectionRow, ...]]:
        return dict(self.table_rows)

    @property
    def games(self) -> Tuple[ProjectionRow, ...]:
        return self.table_rows["games"]

    @property
    def plays(self) -> Tuple[ProjectionRow, ...]:
        return self.table_rows["plays"]

    @property
    def play_events(self) -> Tuple[ProjectionRow, ...]:
        return self.table_rows["play_events"]

    @property
    def pitches(self) -> Tuple[ProjectionRow, ...]:
        return self.table_rows["pitches"]

    @property
    def batted_balls(self) -> Tuple[ProjectionRow, ...]:
        return self.table_rows["batted_balls"]
