"""Client for bounded Baseball Savant Statcast Search CSV exports."""

from datetime import date
import time
from typing import Optional, Sequence, Tuple
from urllib.parse import urlencode, urlsplit

from zavant.ingestion.http import (
    HttpTransport,
    HttpTransportError,
    RetrievedResource,
    RetryPolicy,
    Sleeper,
    UrllibHttpTransport,
)


DEFAULT_BASE_URL = "https://baseballsavant.mlb.com"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_USER_AGENT = "zavant/0.11.0"
QueryParameters = Sequence[Tuple[str, str]]


class BaseballSavantError(RuntimeError):
    """Base class for Baseball Savant client failures."""


class BaseballSavantUnavailableError(BaseballSavantError):
    """Raised when transport failures exhaust all attempts."""


class BaseballSavantResponseError(BaseballSavantError):
    """Raised when Baseball Savant returns a non-successful response."""

    def __init__(self, status_code: int, url: str, attempts: int) -> None:
        self.status_code = status_code
        self.url = url
        self.attempts = attempts
        super().__init__(
            f"Baseball Savant returned HTTP {status_code} for {url} "
            f"after {attempts} attempt(s)"
        )


class BaseballSavantClient:
    """Retrieve all-player regular-season Statcast rows for one game date.

    Each request is deliberately fixed to one inclusive date. Keeping that
    boundary in the client prevents callers from accidentally issuing an
    export large enough to reach Savant's observed 25,000-row truncation limit.
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
            "Accept": "text/csv",
            "User-Agent": user_agent,
        }

    def get_statcast_date(self, game_date: date) -> RetrievedResource:
        """Retrieve all MLB regular-season pitch rows for exactly one date."""

        if type(game_date) is not date:
            raise ValueError("game_date must be a date")
        value = game_date.isoformat()
        return self._get(
            path="/statcast_search/csv",
            query=(
                ("all", "true"),
                ("type", "batter"),
                ("player_type", "batter"),
                ("game_date_gt", value),
                ("game_date_lt", value),
                ("hfGT", "R|"),
            ),
        )

    def _get(self, path: str, query: QueryParameters) -> RetrievedResource:
        request_url = f"{self.base_url}{path}?{urlencode(query)}"
        for attempt in range(1, self.retry_policy.max_attempts + 1):
            try:
                response = self.transport.get(
                    url=request_url,
                    headers=self.request_headers,
                    timeout_seconds=self.timeout_seconds,
                )
            except HttpTransportError as exc:
                if attempt == self.retry_policy.max_attempts:
                    raise BaseballSavantUnavailableError(
                        f"Baseball Savant was unavailable for {request_url} "
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
            raise BaseballSavantResponseError(
                status_code=response.status_code,
                url=response.url,
                attempts=attempt,
            )

        raise AssertionError("retry loop completed without returning or raising")
