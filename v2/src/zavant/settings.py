"""Runtime configuration loaded at the application boundary."""

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Settings:
    """Configuration shared by CLI commands and future services."""

    data_dir: Path
    mlb_api_base_url: str
    s3_bucket: Optional[str]
    s3_prefix: str
    expected_aws_account_id: Optional[str]

    @classmethod
    def from_environment(cls) -> "Settings":
        """Build settings from environment variables and local defaults."""
        return cls(
            data_dir=Path(os.getenv("ZAVANT_DATA_DIR", ".local/lake")),
            mlb_api_base_url=os.getenv(
                "ZAVANT_MLB_API_BASE_URL", "https://statsapi.mlb.com"
            ).rstrip("/"),
            s3_bucket=os.getenv("ZAVANT_S3_BUCKET") or None,
            s3_prefix=os.getenv("ZAVANT_S3_PREFIX", "lake").strip("/"),
            expected_aws_account_id=os.getenv("ZAVANT_EXPECTED_AWS_ACCOUNT_ID")
            or None,
        )
