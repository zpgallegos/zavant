"""Local runtime configuration for the Baseball Savant source."""

from dataclasses import dataclass
import os
from typing import Mapping, Optional

from zavant.ingestion.baseball_savant.client import DEFAULT_BASE_URL


@dataclass(frozen=True)
class BaseballSavantSettings:
    """Operator-selected Baseball Savant client settings."""

    base_url: str = DEFAULT_BASE_URL

    @classmethod
    def from_environment(
        cls,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "BaseballSavantSettings":
        values = os.environ if environ is None else environ
        return cls(
            base_url=values.get("ZAVANT_SAVANT_BASE_URL", DEFAULT_BASE_URL).rstrip(
                "/"
            )
        )
