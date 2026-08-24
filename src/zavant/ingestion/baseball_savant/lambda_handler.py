"""AWS Lambda entry point for isolated Baseball Savant daily acquisition."""

from dataclasses import dataclass
from datetime import date
from importlib import import_module
import os
from typing import Any, Dict, Mapping, Optional, cast

from zavant.ingestion.baseball_savant.daily import BaseballSavantDailyAcquirer
from zavant.ingestion.baseball_savant.client import (
    DEFAULT_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    BaseballSavantClient,
)
from zavant.ingestion.http import RetryPolicy
from zavant.ingestion.baseball_savant.storage import (
    BaseballSavantStore,
    s3_baseball_savant_store,
)
from zavant.storage.s3_objects import S3Client


class BaseballSavantDailyFailedError(RuntimeError):
    """Raised so Lambda records a failed Savant run as a failed invocation."""


@dataclass(frozen=True)
class BaseballSavantLambdaConfiguration:
    """Environment-owned configuration for the Savant Lambda."""

    bucket: str
    initial_date: date
    prefix: str = "lake"
    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_attempts: int = 3
    lookback_days: int = 7
    max_dates_per_run: int = 31

    @classmethod
    def from_environment(
        cls,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "BaseballSavantLambdaConfiguration":
        """Load and validate Savant Lambda configuration."""

        values = os.environ if environ is None else environ
        bucket = values.get("ZAVANT_S3_BUCKET", "").strip()
        if not bucket:
            raise ValueError("ZAVANT_S3_BUCKET must be configured")
        initial_date_value = values.get("ZAVANT_SAVANT_INITIAL_DATE", "").strip()
        if not initial_date_value:
            raise ValueError("ZAVANT_SAVANT_INITIAL_DATE must be configured")
        try:
            initial_date = date.fromisoformat(initial_date_value)
            timeout_seconds = float(
                values.get(
                    "ZAVANT_SAVANT_HTTP_TIMEOUT_SECONDS",
                    str(DEFAULT_TIMEOUT_SECONDS),
                )
            )
            max_attempts = int(values.get("ZAVANT_SAVANT_HTTP_MAX_ATTEMPTS", "3"))
            lookback_days = int(values.get("ZAVANT_SAVANT_LOOKBACK_DAYS", "7"))
            max_dates_per_run = int(values.get("ZAVANT_SAVANT_MAX_DATES_PER_RUN", "31"))
        except ValueError as exc:
            raise ValueError(
                "Savant Lambda configuration has an invalid value"
            ) from exc
        return cls(
            bucket=bucket,
            initial_date=initial_date,
            prefix=values.get("ZAVANT_S3_PREFIX", "lake").strip("/"),
            base_url=values.get("ZAVANT_SAVANT_BASE_URL", DEFAULT_BASE_URL).rstrip("/"),
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            lookback_days=lookback_days,
            max_dates_per_run=max_dates_per_run,
        )

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        for name in ("max_attempts", "lookback_days", "max_dates_per_run"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True)
class BaseballSavantLambdaApplication:
    """Runnable Savant application with dependencies composed once."""

    acquirer: BaseballSavantDailyAcquirer
    store: BaseballSavantStore
    configuration: BaseballSavantLambdaConfiguration

    def run(self, event: Mapping[str, Any]) -> Dict[str, Any]:
        """Execute one daily Savant acquisition invocation."""

        result = self.acquirer.run(
            initial_date=self.configuration.initial_date,
            through_date=_event_through_date(event),
            lookback_days=self.configuration.lookback_days,
            max_dates_per_run=self.configuration.max_dates_per_run,
        )
        payload = result.as_dict()
        if not result.successful:
            raise BaseballSavantDailyFailedError(
                f"Savant daily acquisition failed; inspect {result.manifest_path}"
            )
        return payload


def build_baseball_savant_lambda_application() -> BaseballSavantLambdaApplication:
    """Compose the production Savant application from managed services."""

    configuration = BaseballSavantLambdaConfiguration.from_environment()
    client = BaseballSavantClient(
        base_url=configuration.base_url,
        timeout_seconds=configuration.timeout_seconds,
        retry_policy=RetryPolicy(max_attempts=configuration.max_attempts),
    )
    store = s3_baseball_savant_store(
        client=_boto3_s3_client(),
        bucket=configuration.bucket,
        prefix=configuration.prefix,
    )
    return BaseballSavantLambdaApplication(
        acquirer=BaseballSavantDailyAcquirer(client, store),
        store=store,
        configuration=configuration,
    )


_application: Optional[BaseballSavantLambdaApplication] = None


def lambda_handler(event: Mapping[str, Any], context: object) -> Dict[str, Any]:
    """Run the isolated Baseball Savant daily acquisition process."""

    del context
    global _application
    if _application is None:
        _application = build_baseball_savant_lambda_application()
    return _application.run(event)


def _event_through_date(event: Mapping[str, Any]) -> Optional[date]:
    value = event.get("savant_through_date")
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("event savant_through_date must be an ISO date string")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            "event savant_through_date must use YYYY-MM-DD format"
        ) from exc


def _boto3_s3_client() -> S3Client:
    boto3 = import_module("boto3")
    client_factory = boto3.client
    return cast(S3Client, client_factory("s3"))
