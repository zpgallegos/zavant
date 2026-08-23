"""Source-neutral HTTP transport and retry primitives for ingestion clients."""

from dataclasses import dataclass
import socket
from typing import Callable, Mapping, Optional, Protocol, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


RETRYABLE_STATUS_CODES = (408, 425, 429, 500, 502, 503, 504)

Sleeper = Callable[[float], None]


class HttpTransportError(RuntimeError):
    """Raised when no HTTP response can be obtained from a source."""


@dataclass(frozen=True)
class HttpResponse:
    """Transport-level HTTP response."""

    status_code: int
    body: bytes
    headers: Mapping[str, str]
    url: str


class HttpTransport(Protocol):
    """Minimal injectable HTTP GET transport."""

    def get(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        """Execute one HTTP GET request, including non-success responses."""

        ...


class UrllibHttpTransport:
    """Standard-library implementation of the injectable HTTP transport."""

    def get(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        request = Request(url=url, headers=dict(headers), method="GET")
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                response_headers = {
                    key: value for key, value in response.headers.items()
                }
                return HttpResponse(
                    status_code=int(response.getcode()),
                    body=response.read(),
                    headers=response_headers,
                    url=response.geturl(),
                )
        except HTTPError as exc:
            response_headers = (
                {key: value for key, value in exc.headers.items()}
                if exc.headers is not None
                else {}
            )
            return HttpResponse(
                status_code=exc.code,
                body=exc.read(),
                headers=response_headers,
                url=exc.geturl(),
            )
        except (URLError, TimeoutError, socket.timeout, OSError) as exc:
            raise HttpTransportError(
                f"HTTP request failed before receiving a response: {url}"
            ) from exc


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded exponential retry behavior for transient failures."""

    max_attempts: int = 3
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 4.0
    retryable_status_codes: Tuple[int, ...] = RETRYABLE_STATUS_CODES

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or self.max_attempts <= 0:
            raise ValueError("max_attempts must be a positive integer")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must not be negative")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError(
                "max_delay_seconds must not be less than base_delay_seconds"
            )
        if not self.retryable_status_codes:
            raise ValueError("retryable_status_codes must not be empty")

    def delay_after(
        self,
        failed_attempt: int,
        response_headers: Optional[Mapping[str, str]] = None,
    ) -> float:
        """Calculate the bounded delay after a retryable failure."""

        exponential_delay = self.base_delay_seconds * (2 ** (failed_attempt - 1))
        delay = min(exponential_delay, self.max_delay_seconds)
        retry_after = self._retry_after_seconds(response_headers)
        if retry_after is not None:
            delay = max(delay, min(retry_after, self.max_delay_seconds))
        return delay

    @staticmethod
    def _retry_after_seconds(
        response_headers: Optional[Mapping[str, str]],
    ) -> Optional[float]:
        if response_headers is None:
            return None
        retry_after_value = next(
            (
                value
                for key, value in response_headers.items()
                if key.lower() == "retry-after"
            ),
            None,
        )
        if retry_after_value is None:
            return None
        try:
            retry_after = float(retry_after_value)
        except ValueError:
            return None
        return retry_after if retry_after >= 0 else None


@dataclass(frozen=True)
class RetrievedResource:
    """Successful source response returned to contracts and persistence."""

    body: bytes
    request_url: str
    response_url: str
    status_code: int
    headers: Mapping[str, str]
    attempts: int

    @property
    def source_uri(self) -> str:
        return self.request_url
