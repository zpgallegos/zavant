"""Reliable, storage-neutral client for public MLB Stats API resources."""

from datetime import date, datetime, timezone
import time
from typing import Optional, Sequence, Tuple
from urllib.parse import urlencode, urlsplit

from zavant.ingestion.http import HttpResponse as HttpResponse
from zavant.ingestion.http import (
    HttpTransport,
    HttpTransportError,
    RetrievedResource,
    RetryPolicy,
    Sleeper,
    UrllibHttpTransport,
)


DEFAULT_BASE_URL = "https://statsapi.mlb.com"
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_USER_AGENT = "zavant/0.11.0"
QueryParameters = Sequence[Tuple[str, str]]


class MlbStatsApiError(RuntimeError):
    """Base class for MLB Stats API client failures."""


class MlbStatsApiTransportError(HttpTransportError, MlbStatsApiError):
    """Raised when no HTTP response can be obtained from MLB."""


class MlbStatsApiUnavailableError(MlbStatsApiError):
    """Raised after retryable transport failures exhaust all attempts."""


class MlbStatsApiResponseError(MlbStatsApiError):
    """Raised when MLB returns a non-successful HTTP response."""

    def __init__(self, status_code: int, url: str, attempts: int) -> None:
        self.status_code = status_code
        self.url = url
        self.attempts = attempts
        super().__init__(
            f"MLB Stats API returned HTTP {status_code} for {url} "
            f"after {attempts} attempt(s)"
        )

class MlbStatsApiClient:
    """Typed public-resource client for the MLB Stats API.

    Args:
        base_url: MLB Stats API server without a query or fragment.
        timeout_seconds: Per-attempt HTTP timeout.
        retry_policy: Bounded retry behavior for transient failures.
        transport: Injectable HTTP transport. Defaults to `urllib`.
        sleeper: Injectable delay function used between attempts.
        user_agent: HTTP User-Agent identifying this client.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        retry_policy: Optional[RetryPolicy] = None,
        transport: Optional[HttpTransport] = None,
        sleeper: Sleeper = time.sleep,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        normalized_base_url = base_url.rstrip("/")
        parsed_base_url = urlsplit(normalized_base_url)
        if (
            parsed_base_url.scheme not in {"http", "https"}
            or not parsed_base_url.netloc
        ):
            raise ValueError("base_url must be an absolute HTTP(S) URL")
        if parsed_base_url.query or parsed_base_url.fragment:
            raise ValueError("base_url must not contain a query or fragment")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not user_agent:
            raise ValueError("user_agent must not be empty")

        self.base_url = normalized_base_url
        self.timeout_seconds = timeout_seconds
        self.retry_policy = retry_policy if retry_policy is not None else RetryPolicy()
        self.transport = transport or UrllibHttpTransport()
        self.sleeper = sleeper
        self.request_headers = {
            "Accept": "application/json",
            "User-Agent": user_agent,
        }

    def get_schedule(
        self,
        start_date: date,
        end_date: date,
        sport_id: int = 1,
    ) -> RetrievedResource:
        """Retrieve one bounded MLB schedule response.

        Args:
            start_date: Inclusive first official date requested.
            end_date: Inclusive last official date requested.
            sport_id: MLB sport identifier, with `1` representing MLB.

        Returns:
            Successful response bytes and HTTP provenance.

        Raises:
            ValueError: If date boundaries or sport ID are invalid.
            MlbStatsApiError: If the request cannot complete successfully.
        """

        if start_date > end_date:
            raise ValueError("start_date must not be after end_date")
        self._validate_positive_integer(sport_id, "sport_id")
        return self._get(
            path="/api/v1/schedule",
            query=(
                ("sportId", str(sport_id)),
                ("startDate", start_date.isoformat()),
                ("endDate", end_date.isoformat()),
            ),
        )

    def get_game_changes(
        self,
        updated_since: datetime,
        sport_id: int = 1,
        limit: int = 1000,
        offset: int = 0,
    ) -> RetrievedResource:
        """Retrieve one page of games changed since a UTC watermark.

        Args:
            updated_since: Inclusive correction watermark with a UTC offset.
            sport_id: MLB sport identifier, with `1` representing MLB.
            limit: Maximum results requested from this page.
            offset: Result offset for this page.

        Returns:
            Successful response bytes and HTTP provenance.

        Raises:
            ValueError: If timestamp or pagination values are invalid.
            MlbStatsApiError: If the request cannot complete successfully.
        """

        if updated_since.tzinfo is None or updated_since.utcoffset() is None:
            raise ValueError("updated_since must include a UTC offset")
        self._validate_positive_integer(sport_id, "sport_id")
        self._validate_positive_integer(limit, "limit")
        if type(offset) is not int or offset < 0:
            raise ValueError("offset must be a non-negative integer")
        return self._get(
            path="/api/v1/game/changes",
            query=(
                ("updatedSince", self._format_utc_timestamp(updated_since)),
                ("sportId", str(sport_id)),
                ("limit", str(limit)),
                ("offset", str(offset)),
            ),
        )

    def get_live_game(self, game_pk: int) -> RetrievedResource:
        """Retrieve the complete live feed for one MLB game.

        Args:
            game_pk: MLB's primary identifier for the game.

        Returns:
            Successful response bytes and HTTP provenance.

        Raises:
            ValueError: If the game identifier is invalid.
            MlbStatsApiError: If the request cannot complete successfully.
        """

        self._validate_positive_integer(game_pk, "game_pk")
        return self._get(path=f"/api/v1.1/game/{game_pk}/feed/live", query=())

    def _get(self, path: str, query: QueryParameters) -> RetrievedResource:
        """Execute a GET request using the configured retry policy.

        Args:
            path: Absolute API path below the configured server.
            query: Ordered query-string name/value pairs.

        Returns:
            Successful response bytes and HTTP provenance.

        Raises:
            MlbStatsApiUnavailableError: If transport failures exhaust retries.
            MlbStatsApiResponseError: If the final response is not successful.
        """

        query_string = urlencode(query)
        request_url = f"{self.base_url}{path}"
        if query_string:
            request_url = f"{request_url}?{query_string}"

        for attempt in range(1, self.retry_policy.max_attempts + 1):
            try:
                response = self.transport.get(
                    url=request_url,
                    headers=self.request_headers,
                    timeout_seconds=self.timeout_seconds,
                )
            except HttpTransportError as exc:
                if attempt == self.retry_policy.max_attempts:
                    raise MlbStatsApiUnavailableError(
                        f"MLB Stats API was unavailable for {request_url} "
                        f"after {attempt} attempt(s)"
                    ) from exc
                self.sleeper(self.retry_policy.delay_after(attempt))
                continue

            if 200 <= response.status_code < 300:
                return RetrievedResource(
                    body=response.body,
                    request_url=request_url,
                    response_url=response.url,
                    status_code=response.status_code,
                    headers=response.headers,
                    attempts=attempt,
                )

            retryable = response.status_code in self.retry_policy.retryable_status_codes
            if retryable and attempt < self.retry_policy.max_attempts:
                self.sleeper(self.retry_policy.delay_after(attempt, response.headers))
                continue
            raise MlbStatsApiResponseError(
                status_code=response.status_code,
                url=response.url,
                attempts=attempt,
            )

        raise AssertionError("retry loop completed without returning or raising")

    @staticmethod
    def _format_utc_timestamp(value: datetime) -> str:
        normalized = value.astimezone(timezone.utc)
        timespec = "microseconds" if normalized.microsecond else "seconds"
        return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")

    @staticmethod
    def _validate_positive_integer(value: int, name: str) -> None:
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
