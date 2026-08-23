"""Values exchanged by Baseball Savant projection components."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Tuple

from zavant.projection.contracts import ProjectionRow


@dataclass(frozen=True)
class StatcastProjectionSource:
    """Validated raw date revision and its immutable provenance."""

    game_date: date
    revision_id: str
    observed_at: datetime
    source_uri: str
    raw_object_uri: str
    raw: bytes


@dataclass(frozen=True)
class StatcastDateProjection:
    """Analytical rows produced from one Savant date revision."""

    table_rows: Dict[str, Tuple[ProjectionRow, ...]]

    @property
    def batting_events(self) -> Tuple[ProjectionRow, ...]:
        return self.table_rows["statcast_batting_events"]

    @property
    def dates(self) -> Tuple[ProjectionRow, ...]:
        return self.table_rows["statcast_dates"]
