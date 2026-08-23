"""Local runtime configuration for the MLB Stats API source."""

from dataclasses import dataclass
import os
from typing import Mapping, Optional

from zavant.ingestion.mlb_stats_api.client import DEFAULT_BASE_URL


@dataclass(frozen=True)
class MlbStatsApiSettings:
    """Operator-selected MLB Stats API client settings."""

    base_url: str = DEFAULT_BASE_URL

    @classmethod
    def from_environment(
        cls,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "MlbStatsApiSettings":
        values = os.environ if environ is None else environ
        return cls(
            base_url=values.get("ZAVANT_MLB_API_BASE_URL", DEFAULT_BASE_URL).rstrip(
                "/"
            )
        )
