from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
from typing import Dict, List, Optional, Tuple
import unittest
from uuid import UUID

from zavant.acquisition.season_backfill import (
    SeasonBackfillCoordinator,
    SeasonBackfillMode,
)
from zavant.clients.mlb_stats_api import RetrievedResource
from zavant.contracts.raw_game import RawGameResponse
from zavant.storage.path_raw import PathRawGameStore
from zavant.storage.path_schedule import PathScheduleStore
from zavant.storage.path_season_backfills import PathSeasonBackfillStore


SEASON = 2024
STARTED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000031")


def retrieved(body: bytes, uri: str) -> RetrievedResource:
    return RetrievedResource(
        body=body,
        request_url=uri,
        response_url=uri,
        status_code=200,
        headers={},
        attempts=1,
    )


def raw_game(game_pk: int, marker: int = 1) -> bytes:
    return json.dumps(
        {
            "gamePk": game_pk,
            "metaData": {"timeStamp": f"20240401_00000{marker}"},
            "gameData": {
                "datetime": {"officialDate": "2024-04-01"},
                "status": {"codedGameState": "F", "detailedState": "Final"},
            },
            "liveData": {"marker": marker},
        }
    ).encode()


def schedule(games: Tuple[int, ...]) -> bytes:
    entries = [
        {
            "gamePk": game_pk,
            "gameDate": "2024-04-01T17:00:00Z",
            "officialDate": "2024-04-01",
            "season": "2024",
            "gameType": "R",
            "link": f"/api/v1.1/game/{game_pk}/feed/live",
            "status": {"codedGameState": "F", "detailedState": "Final"},
        }
        for game_pk in games
    ]
    payload = {
        "totalItems": len(entries),
        "totalGames": len(entries),
        "dates": ([{"date": "2024-04-01", "games": entries}] if entries else []),
    }
    return json.dumps(payload).encode()


def changes(games: Tuple[int, ...]) -> bytes:
    entries = [
        {
            "gamePk": game_pk,
            "officialDate": "2024-04-01",
            "season": 2024,
            "link": f"/api/v1.1/game/{game_pk}/feed/live",
            "status": {"codedGameState": "F", "detailedState": "Final"},
        }
        for game_pk in games
    ]
    payload = {
        "totalItems": len(entries),
        "totalGames": len(entries),
        "dates": ([{"date": "2024-04-01", "games": entries}] if entries else []),
    }
    return json.dumps(payload).encode()


class FakeBackfillApi:
    def __init__(
        self,
        game_responses: Dict[int, bytes],
        changed_games: Tuple[int, ...] = (),
        scheduled_games_by_month: Optional[Dict[int, Tuple[int, ...]]] = None,
    ) -> None:
        self.game_responses = game_responses
        self.changed_games = changed_games
        self.scheduled_games_by_month = scheduled_games_by_month or {4: (1, 2)}
        self.schedule_calls: List[Tuple[date, date, int]] = []
        self.game_calls: List[int] = []
        self.change_calls: List[Tuple[datetime, int, int, int]] = []

    def get_schedule(
        self, start_date: date, end_date: date, sport_id: int = 1
    ) -> RetrievedResource:
        self.schedule_calls.append((start_date, end_date, sport_id))
        games = self.scheduled_games_by_month.get(start_date.month, ())
        return retrieved(schedule(games), "https://example.test/api/v1/schedule")

    def get_live_game(self, game_pk: int) -> RetrievedResource:
        self.game_calls.append(game_pk)
        return retrieved(
            self.game_responses[game_pk],
            f"https://example.test/api/v1.1/game/{game_pk}/feed/live",
        )

    def get_game_changes(
        self,
        updated_since: datetime,
        sport_id: int = 1,
        limit: int = 1000,
        offset: int = 0,
    ) -> RetrievedResource:
        self.change_calls.append((updated_since, sport_id, limit, offset))
        return retrieved(
            changes(self.changed_games),
            "https://example.test/api/v1/game/changes",
        )


class SeasonBackfillCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.game_store = PathRawGameStore(
            self.data_dir, clock=lambda: OBSERVED_AT
        )
        self.schedule_store = PathScheduleStore(
            self.data_dir, clock=lambda: STARTED_AT + timedelta(minutes=1)
        )
        self.backfill_store = PathSeasonBackfillStore(
            self.data_dir, clock=lambda: STARTED_AT + timedelta(minutes=1)
        )

    def coordinator(self, api: FakeBackfillApi) -> SeasonBackfillCoordinator:
        return SeasonBackfillCoordinator(
            api=api,
            schedule_store=self.schedule_store,
            game_store=self.game_store,
            backfill_store=self.backfill_store,
            clock=lambda: STARTED_AT,
            run_id_factory=lambda: RUN_ID,
        )

    def land_existing(self, game_pk: int = 1) -> None:
        raw = raw_game(game_pk)
        self.game_store.land(
            RawGameResponse.from_bytes(raw),
            raw,
            "https://example.test/existing",
            "initial",
        )

    def test_missing_mode_downloads_only_absent_games(self) -> None:
        self.land_existing()
        api = FakeBackfillApi({2: raw_game(2)})

        result = self.coordinator(api).run(
            (SEASON,), mode=SeasonBackfillMode.MISSING
        )

        self.assertTrue(result.successful)
        self.assertEqual(api.game_calls, [2])
        self.assertEqual(api.change_calls, [])
        self.assertEqual(len(api.schedule_calls), 12)
        self.assertIsNone(self.backfill_store.read_checkpoint(SEASON))

    def test_reconcile_downloads_missing_and_publicly_changed_games(self) -> None:
        self.land_existing()
        api = FakeBackfillApi(
            {1: raw_game(1, marker=2), 2: raw_game(2)},
            changed_games=(1,),
        )

        result = self.coordinator(api).run(
            (SEASON,), mode=SeasonBackfillMode.RECONCILE
        )

        self.assertTrue(result.successful)
        self.assertEqual(api.game_calls, [1, 2])
        self.assertEqual(len(api.change_calls), 1)
        self.assertEqual(
            api.change_calls[0][0], OBSERVED_AT - timedelta(minutes=5)
        )
        checkpoint = self.backfill_store.read_checkpoint(SEASON)
        self.assertIsNotNone(checkpoint)
        assert checkpoint is not None
        self.assertEqual(checkpoint.updated_since, STARTED_AT)
        evidence = list(
            self.data_dir.rglob("backfill_game_changes/**/response.json")
        )
        self.assertEqual(len(evidence), 1)

    def test_verify_redownloads_all_games_and_content_addressing_noops(self) -> None:
        self.land_existing()
        existing_two = raw_game(2)
        self.game_store.land(
            RawGameResponse.from_bytes(existing_two),
            existing_two,
            "https://example.test/existing/2",
            "initial",
        )
        api = FakeBackfillApi({1: raw_game(1), 2: existing_two})

        result = self.coordinator(api).run(
            (SEASON,), mode=SeasonBackfillMode.VERIFY
        )

        self.assertTrue(result.successful)
        self.assertEqual(api.game_calls, [1, 2])
        self.assertEqual(api.change_calls, [])
        self.assertEqual(len(list(self.data_dir.rglob("game.json"))), 2)

    def test_resume_reuses_schedules_and_completed_correction_evidence(self) -> None:
        self.land_existing()
        api = FakeBackfillApi(
            {1: raw_game(1, marker=2), 2: raw_game(99)},
            changed_games=(1,),
        )

        first = self.coordinator(api).run(
            (SEASON,),
            mode=SeasonBackfillMode.RECONCILE,
            run_id=RUN_ID,
            started_at=STARTED_AT,
        )
        api.game_responses[2] = raw_game(2)
        second = self.coordinator(api).run(
            (SEASON,),
            mode=SeasonBackfillMode.RECONCILE,
            run_id=RUN_ID,
            started_at=STARTED_AT,
        )

        self.assertFalse(first.successful)
        self.assertTrue(second.successful)
        self.assertTrue(second.resumed)
        self.assertEqual(len(api.schedule_calls), 12)
        self.assertEqual(len(api.change_calls), 1)
        self.assertEqual(api.game_calls, [1, 2, 2])
        manifest = json.loads(Path(second.manifest_path.uri).read_text())
        details = manifest["season_runs"][0]["details"]
        self.assertEqual(details["selected"], 2)
        self.assertEqual(details["downloaded"], 2)

    def test_dry_run_plans_without_downloading_or_advancing_state(self) -> None:
        api = FakeBackfillApi({})

        result = self.coordinator(api).run(
            (SEASON,), mode=SeasonBackfillMode.RECONCILE, dry_run=True
        )

        self.assertTrue(result.successful)
        self.assertEqual(api.game_calls, [])
        self.assertIsNone(self.backfill_store.read_checkpoint(SEASON))
        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        details = manifest["season_runs"][0]["details"]
        self.assertEqual(details["selected"], 2)
        self.assertEqual(details["downloaded"], 0)

    def test_rescheduled_game_in_multiple_months_is_downloaded_once(self) -> None:
        api = FakeBackfillApi(
            {1: raw_game(1)},
            scheduled_games_by_month={4: (1,), 8: (1,)},
        )

        result = self.coordinator(api).run(
            (SEASON,), mode=SeasonBackfillMode.MISSING
        )

        self.assertTrue(result.successful)
        self.assertEqual(api.game_calls, [1])
        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        details = manifest["season_runs"][0]["details"]
        self.assertEqual(details["selected"], 1)
        self.assertEqual(details["downloaded"], 1)


if __name__ == "__main__":
    unittest.main()
