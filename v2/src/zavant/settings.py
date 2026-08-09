"""Runtime configuration loaded at the application boundary."""

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Configuration shared by CLI commands and future services.

    Attributes:
        data_dir: Root directory for locally persisted data.
        mlb_api_base_url: Base URL for MLB Stats API requests.
    """

    data_dir: Path
    mlb_api_base_url: str

    @classmethod
    def from_environment(cls) -> "Settings":
        """Build settings from environment variables and local defaults.

        Returns:
            The resolved runtime settings.
        """

        return cls(
            data_dir=Path(os.getenv("ZAVANT_DATA_DIR", ".local/lake")),
            mlb_api_base_url=os.getenv(
                "ZAVANT_MLB_API_BASE_URL", "https://statsapi.mlb.com"
            ).rstrip("/"),
        )
