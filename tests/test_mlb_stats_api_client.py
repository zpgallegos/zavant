from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Mapping, Optional
import unittest
from urllib.parse import parse_qs, urlsplit

from zavant.clients.mlb_stats_api import (
    HttpResponse,
    MlbStatsApiClient,
    MlbStatsApiResponseError,
    MlbStatsApiTransportError,
    MlbStatsApiUnavailableError,
    RetryPolicy,
)
from zavant.contracts.schedule import ScheduleResponse


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_SCHEDULE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-schedule.json"
TEST_BASE_URL = "https://statsapi.example.test"


@dataclass(frozen=True)
class TransportCall:
    url: str
    headers: Mapping[str, str]
    timeout_seconds: float


class FakeTransport:
    def __init__(self, outcomes: List[object]) -> None:
        self.outcomes = list(outcomes)
        self.calls: List[TransportCall] = []

    def get(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        self.calls.append(
            TransportCall(
                url=url,
                headers=dict(headers),
                timeout_seconds=timeout_seconds,
            )
        )
        if not self.outcomes:
            raise AssertionError("fake transport has no queued outcome")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, MlbStatsApiTransportError):
            raise outcome
        if not isinstance(outcome, HttpResponse):
            raise AssertionError("fake transport outcome has an invalid type")
        return outcome


def response(
    status_code: int = 200,
    body: bytes = b"{}",
    headers: Optional[Mapping[str, str]] = None,
    url: str = TEST_BASE_URL,
) -> HttpResponse:
    return HttpResponse(
        status_code=status_code,
        body=body,
        headers=headers or {},
        url=url,
    )


class MlbStatsApiClientRequestTests(unittest.TestCase):
    def test_builds_bounded_schedule_request(self) -> None:
        transport = FakeTransport([response(body=SAMPLE_SCHEDULE.read_bytes())])
        client = MlbStatsApiClient(
            base_url=TEST_BASE_URL,
            timeout_seconds=7.5,
            transport=transport,
        )

        retrieved = client.get_schedule(
            start_date=date(2026, 8, 8),
            end_date=date(2026, 8, 9),
        )

        call = transport.calls[0]
        parsed_url = urlsplit(call.url)
        self.assertEqual(parsed_url.path, "/api/v1/schedule")
        self.assertEqual(
            parse_qs(parsed_url.query),
            {
                "endDate": ["2026-08-09"],
                "sportId": ["1"],
                "startDate": ["2026-08-08"],
            },
        )
        self.assertEqual(call.timeout_seconds, 7.5)
        self.assertEqual(call.headers["Accept"], "application/json")
        self.assertIn("zavant/", call.headers["User-Agent"])
        self.assertEqual(retrieved.body, SAMPLE_SCHEDULE.read_bytes())
        self.assertEqual(retrieved.source_uri, call.url)

    def test_builds_paginated_game_changes_request(self) -> None:
        transport = FakeTransport([response()])
        client = MlbStatsApiClient(base_url=TEST_BASE_URL, transport=transport)
        pacific_time = timezone(timedelta(hours=-7))

        client.get_game_changes(
            updated_since=datetime(2026, 8, 8, 17, 30, tzinfo=pacific_time),
            limit=250,
            offset=500,
        )

        parsed_url = urlsplit(transport.calls[0].url)
        self.assertEqual(parsed_url.path, "/api/v1/game/changes")
        self.assertEqual(
            parse_qs(parsed_url.query),
            {
                "limit": ["250"],
                "offset": ["500"],
                "sportId": ["1"],
                "updatedSince": ["2026-08-09T00:30:00Z"],
            },
        )

    def test_builds_complete_live_game_request(self) -> None:
        transport = FakeTransport([response()])
        client = MlbStatsApiClient(base_url=TEST_BASE_URL, transport=transport)

        client.get_live_game(823514)

        self.assertEqual(
            transport.calls[0].url,
            f"{TEST_BASE_URL}/api/v1.1/game/823514/feed/live",
        )

    def test_returns_bytes_for_a_contract_to_validate(self) -> None:
        transport = FakeTransport([response(body=SAMPLE_SCHEDULE.read_bytes())])
        client = MlbStatsApiClient(base_url=TEST_BASE_URL, transport=transport)

        retrieved = client.get_schedule(
            start_date=date(2026, 8, 8),
            end_date=date(2026, 8, 8),
        )
        schedule = ScheduleResponse.from_bytes(retrieved.body)

        self.assertEqual(schedule.game_pks, (823514, 824726))

    def test_rejects_naive_change_watermark_before_transport(self) -> None:
        transport = FakeTransport([])
        client = MlbStatsApiClient(base_url=TEST_BASE_URL, transport=transport)

        with self.assertRaisesRegex(ValueError, "UTC offset"):
            client.get_game_changes(datetime(2026, 8, 9))

        self.assertEqual(transport.calls, [])


class MlbStatsApiClientRetryTests(unittest.TestCase):
    def test_retries_transient_response_and_honors_retry_after(self) -> None:
        transport = FakeTransport(
            [
                response(status_code=503, headers={"Retry-After": "2"}),
                response(body=b'{"ok": true}'),
            ]
        )
        delays: List[float] = []
        client = MlbStatsApiClient(
            base_url=TEST_BASE_URL,
            transport=transport,
            retry_policy=RetryPolicy(
                max_attempts=3,
                base_delay_seconds=0.25,
                max_delay_seconds=1.0,
            ),
            sleeper=delays.append,
        )

        retrieved = client.get_live_game(823514)

        self.assertEqual(retrieved.attempts, 2)
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(delays, [1.0])

    def test_retries_transport_failure(self) -> None:
        transport = FakeTransport(
            [MlbStatsApiTransportError("connection reset"), response()]
        )
        delays: List[float] = []
        client = MlbStatsApiClient(
            base_url=TEST_BASE_URL,
            transport=transport,
            retry_policy=RetryPolicy(
                max_attempts=2,
                base_delay_seconds=0.25,
                max_delay_seconds=1.0,
            ),
            sleeper=delays.append,
        )

        retrieved = client.get_live_game(823514)

        self.assertEqual(retrieved.attempts, 2)
        self.assertEqual(delays, [0.25])

    def test_does_not_retry_non_transient_response(self) -> None:
        transport = FakeTransport([response(status_code=404)])
        delays: List[float] = []
        client = MlbStatsApiClient(
            base_url=TEST_BASE_URL,
            transport=transport,
            sleeper=delays.append,
        )

        with self.assertRaises(MlbStatsApiResponseError) as raised:
            client.get_live_game(823514)

        self.assertEqual(raised.exception.status_code, 404)
        self.assertEqual(raised.exception.attempts, 1)
        self.assertEqual(delays, [])

    def test_stops_after_configured_response_attempts(self) -> None:
        transport = FakeTransport(
            [
                response(status_code=503),
                response(status_code=503),
                response(status_code=503),
            ]
        )
        delays: List[float] = []
        client = MlbStatsApiClient(
            base_url=TEST_BASE_URL,
            transport=transport,
            retry_policy=RetryPolicy(
                max_attempts=3,
                base_delay_seconds=0.25,
                max_delay_seconds=1.0,
            ),
            sleeper=delays.append,
        )

        with self.assertRaises(MlbStatsApiResponseError) as raised:
            client.get_live_game(823514)

        self.assertEqual(raised.exception.attempts, 3)
        self.assertEqual(delays, [0.25, 0.5])

    def test_classifies_exhausted_transport_failures_as_unavailable(self) -> None:
        transport = FakeTransport(
            [
                MlbStatsApiTransportError("timeout"),
                MlbStatsApiTransportError("timeout"),
            ]
        )
        client = MlbStatsApiClient(
            base_url=TEST_BASE_URL,
            transport=transport,
            retry_policy=RetryPolicy(
                max_attempts=2,
                base_delay_seconds=0,
                max_delay_seconds=0,
            ),
            sleeper=lambda _: None,
        )

        with self.assertRaises(MlbStatsApiUnavailableError):
            client.get_live_game(823514)
