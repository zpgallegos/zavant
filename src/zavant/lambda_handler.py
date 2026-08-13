"""AWS Lambda entry point and composition boundary for daily acquisition."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from importlib import import_module
import os
from typing import Any, Callable, Dict, Mapping, Optional, cast

from zavant.acquisition.daily import DailyAcquisitionCoordinator, utc_now
from zavant.application import MlbDailyApi, build_daily_coordinator
from zavant.clients.mlb_stats_api import (
    DEFAULT_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    MlbStatsApiClient,
    RetryPolicy,
)
from zavant.storage.bundles import AcquisitionStorage, s3_acquisition_storage
from zavant.storage.s3_objects import S3Client


Clock = Callable[[], datetime]


class DailyAcquisitionFailedError(RuntimeError):
    """Raised so Lambda records an unsuccessful daily run as a failed invocation."""


@dataclass(frozen=True)
class LambdaConfiguration:
    """Environment-owned configuration for the scheduled Lambda application."""

    bucket: str
    initial_schedule_date: date
    initial_correction_watermark: datetime
    prefix: str = "lake"
    mlb_api_base_url: str = DEFAULT_BASE_URL
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_attempts: int = 3
    schedule_lookback_days: int = 7
    correction_overlap_seconds: float = 300.0
    correction_limit: int = 1000
    correction_max_pages: int = 100
    sport_id: int = 1

    @classmethod
    def from_environment(
        cls,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "LambdaConfiguration":
        """Load and validate Lambda configuration from environment variables.

        Args:
            environ: Environment mapping; defaults to the process environment.

        Returns:
            Validated Lambda configuration.

        Raises:
            ValueError: If required or typed configuration is invalid.
        """

        values = os.environ if environ is None else environ
        bucket = values.get("ZAVANT_S3_BUCKET", "").strip()
        if not bucket:
            raise ValueError("ZAVANT_S3_BUCKET must be configured")
        return cls(
            bucket=bucket,
            prefix=values.get("ZAVANT_S3_PREFIX", "lake").strip("/"),
            mlb_api_base_url=values.get(
                "ZAVANT_MLB_API_BASE_URL", DEFAULT_BASE_URL
            ).rstrip("/"),
            timeout_seconds=_float_value(
                values, "ZAVANT_HTTP_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS
            ),
            max_attempts=_integer_value(values, "ZAVANT_HTTP_MAX_ATTEMPTS", 3),
            initial_schedule_date=_required_date(
                values, "ZAVANT_INITIAL_SCHEDULE_DATE"
            ),
            initial_correction_watermark=_required_timestamp(
                values, "ZAVANT_INITIAL_CORRECTION_WATERMARK"
            ),
            schedule_lookback_days=_integer_value(
                values, "ZAVANT_SCHEDULE_LOOKBACK_DAYS", 7
            ),
            correction_overlap_seconds=_float_value(
                values, "ZAVANT_CORRECTION_OVERLAP_SECONDS", 300.0
            ),
            correction_limit=_integer_value(
                values, "ZAVANT_CORRECTION_LIMIT", 1000
            ),
            correction_max_pages=_integer_value(
                values, "ZAVANT_CORRECTION_MAX_PAGES", 100
            ),
            sport_id=_integer_value(values, "ZAVANT_SPORT_ID", 1),
        )

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        for name in (
            "max_attempts",
            "correction_limit",
            "correction_max_pages",
            "sport_id",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.schedule_lookback_days < 0:
            raise ValueError("schedule_lookback_days must not be negative")
        if self.correction_overlap_seconds < 0:
            raise ValueError("correction_overlap_seconds must not be negative")


@dataclass(frozen=True)
class LambdaApplication:
    """Runnable daily application with dependencies composed once per runtime."""

    coordinator: DailyAcquisitionCoordinator
    storage: AcquisitionStorage
    configuration: LambdaConfiguration

    def run(self, event: Mapping[str, Any]) -> Dict[str, Any]:
        """Execute one daily acquisition invocation.

        Existing S3 watermarks take precedence over bootstrap configuration, so
        the same warm Lambda application remains valid after its first run.

        Args:
            event: Lambda event, optionally containing an ISO `through_date`.

        Returns:
            JSON-serializable aggregate acquisition result.

        Raises:
            DailyAcquisitionFailedError: If any daily branch failed.
            ValueError: If an event boundary is invalid.
        """

        through_date = _event_through_date(event)
        initial_schedule_date = (
            self.configuration.initial_schedule_date
            if self.storage.schedule_watermark.read() is None
            else None
        )
        initial_correction_watermark = (
            self.configuration.initial_correction_watermark
            if self.storage.game_changes_watermark.read() is None
            else None
        )
        result = self.coordinator.run(
            initial_schedule_date=initial_schedule_date,
            initial_correction_watermark=initial_correction_watermark,
            through_date=through_date,
            schedule_lookback_days=self.configuration.schedule_lookback_days,
            correction_overlap=timedelta(
                seconds=self.configuration.correction_overlap_seconds
            ),
            correction_limit=self.configuration.correction_limit,
            correction_max_pages=self.configuration.correction_max_pages,
            sport_id=self.configuration.sport_id,
        )
        payload = result.as_dict()
        if not result.successful:
            raise DailyAcquisitionFailedError(
                f"daily acquisition failed; inspect {result.manifest_path}"
            )
        return payload


def build_lambda_application(
    environ: Optional[Mapping[str, str]] = None,
    s3_client: Optional[S3Client] = None,
    api: Optional[MlbDailyApi] = None,
    clock: Clock = utc_now,
) -> LambdaApplication:
    """Compose the production daily application from external adapters.

    Args:
        environ: Environment mapping used for configuration.
        s3_client: Injectable S3 client; defaults to a Boto3 client.
        api: Injectable MLB API; defaults to the HTTP client.
        clock: Shared UTC time source.

    Returns:
        Fully composed Lambda application.
    """

    configuration = LambdaConfiguration.from_environment(environ)
    resolved_s3_client = s3_client or _boto3_s3_client()
    resolved_api = api or MlbStatsApiClient(
        base_url=configuration.mlb_api_base_url,
        timeout_seconds=configuration.timeout_seconds,
        retry_policy=RetryPolicy(max_attempts=configuration.max_attempts),
    )
    storage = s3_acquisition_storage(
        client=resolved_s3_client,
        bucket=configuration.bucket,
        prefix=configuration.prefix,
        clock=clock,
    )
    return LambdaApplication(
        coordinator=build_daily_coordinator(resolved_api, storage, clock),
        storage=storage,
        configuration=configuration,
    )


_application: Optional[LambdaApplication] = None


def lambda_handler(event: Mapping[str, Any], context: object) -> Dict[str, Any]:
    """Run daily acquisition from an AWS Lambda invocation.

    The composed application is cached at module scope so warm invocations
    reuse SDK clients and configuration. AWS supplies `context`; acquisition
    does not currently require it.

    Args:
        event: EventBridge or test event with optional `through_date` override.
        context: AWS Lambda invocation context.

    Returns:
        JSON-serializable aggregate acquisition result.
    """

    del context
    global _application
    if _application is None:
        _application = build_lambda_application()
    return _application.run(event)


def _boto3_s3_client() -> S3Client:
    boto3 = import_module("boto3")
    client_factory = boto3.client
    return cast(S3Client, client_factory("s3"))


def _event_through_date(event: Mapping[str, Any]) -> Optional[date]:
    value = event.get("through_date")
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("event through_date must be an ISO date string")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("event through_date must use YYYY-MM-DD format") from exc


def _integer_value(values: Mapping[str, str], name: str, default: int) -> int:
    try:
        return int(values.get(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _float_value(values: Mapping[str, str], name: str, default: float) -> float:
    try:
        return float(values.get(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric") from exc


def _required_date(
    values: Mapping[str, str],
    name: str,
) -> date:
    value = values.get(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} must be configured")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must use YYYY-MM-DD format") from exc


def _required_timestamp(
    values: Mapping[str, str],
    name: str,
) -> datetime:
    value = values.get(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} must be configured")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{name} must use ISO-8601 format") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must include a UTC offset")
    return parsed.astimezone(timezone.utc)
