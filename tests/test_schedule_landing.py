from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from uuid import UUID

from zavant.contracts.schedule import (
    ScheduleContractError,
    ScheduleRequest,
    ScheduleResponse,
)
from zavant.storage.path_schedule import (
    PathScheduleStore,
    ScheduleConflictError,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_SCHEDULE = REPOSITORY_ROOT / "tests" / "fixtures" / "example-schedule.json"
START_DATE = date(2026, 8, 8)
END_DATE = date(2026, 8, 8)
REQUESTED_AT = datetime(2026, 8, 9, 0, 0, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 9, 0, 1, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000002")


class ScheduleContractTests(unittest.TestCase):
    def test_extracts_deduplicated_scheduled_games(self) -> None:
        schedule = ScheduleResponse.from_bytes(SAMPLE_SCHEDULE.read_bytes())

        self.assertEqual(schedule.total_items, 2)
        self.assertEqual(schedule.total_games, 2)
        self.assertEqual(schedule.game_pks, (823514, 824726))
        self.assertEqual(schedule.scheduled_games[0].game_type, "R")
        self.assertEqual(schedule.scheduled_games[0].season, 2026)
        self.assertEqual(
            schedule.scheduled_games[0].scheduled_start,
            datetime(2026, 8, 8, 19, 5, tzinfo=timezone.utc),
        )

    def test_rejects_game_without_live_feed_link(self) -> None:
        payload = json.loads(SAMPLE_SCHEDULE.read_bytes())
        del payload["dates"][0]["games"][0]["link"]

        with self.assertRaisesRegex(ScheduleContractError, "link must be"):
            ScheduleResponse.from_bytes(json.dumps(payload).encode())

    def test_rejects_inconsistent_game_total(self) -> None:
        payload = json.loads(SAMPLE_SCHEDULE.read_bytes())
        payload["totalGames"] = 1

        with self.assertRaisesRegex(ScheduleContractError, "totalGames"):
            ScheduleResponse.from_bytes(json.dumps(payload).encode())

    def test_counts_duplicate_entries_and_keeps_latest_game_state(self) -> None:
        payload = json.loads(SAMPLE_SCHEDULE.read_bytes())
        postponed = dict(payload["dates"][0]["games"][0])
        postponed["gameDate"] = "2026-08-07T19:05:00Z"
        postponed["status"] = {
            "codedGameState": "D",
            "detailedState": "Postponed",
        }
        payload["dates"].append(
            {"date": "2026-08-07", "games": [postponed]}
        )
        payload["totalGames"] = 3
        payload["totalItems"] = 3

        schedule = ScheduleResponse.from_bytes(json.dumps(payload).encode())

        self.assertEqual(schedule.total_games, 3)
        self.assertEqual(schedule.game_pks, (823514, 824726))
        self.assertEqual(schedule.scheduled_games[0].detailed_state, "Final")

    def test_request_requires_ordered_date_boundaries(self) -> None:
        with self.assertRaisesRegex(ValueError, "start_date"):
            ScheduleRequest(
                start_date=date(2026, 8, 9),
                end_date=date(2026, 8, 8),
                sport_id=1,
                requested_at=REQUESTED_AT,
                source_uri="fixture://schedule",
            )


class PathScheduleStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.data_dir = Path(self.temporary_directory.name)
        self.raw = SAMPLE_SCHEDULE.read_bytes()
        self.schedule = ScheduleResponse.from_bytes(self.raw)
        self.store = PathScheduleStore(
            self.data_dir,
            clock=lambda: OBSERVED_AT,
        )

    def request(self, sport_id: int = 1) -> ScheduleRequest:
        return ScheduleRequest(
            start_date=START_DATE,
            end_date=END_DATE,
            sport_id=sport_id,
            requested_at=REQUESTED_AT,
            source_uri="fixture://schedule",
        )

    def test_lands_response_metadata_and_discovery_manifest(self) -> None:
        result = self.store.land(
            schedule=self.schedule,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )

        self.assertTrue(result.created)
        self.assertEqual(Path(result.response_path.uri).read_bytes(), self.raw)
        self.assertIn(
            "schedules/request_date=2026-08-09/"
            "run_id=00000000-0000-0000-0000-000000000002",
            result.response_path.key,
        )

        metadata = json.loads(Path(result.metadata_path.uri).read_text())
        self.assertEqual(metadata["contract"], "mlb-stats-api-schedule-response/v1")
        self.assertEqual(metadata["request"]["start_date"], "2026-08-08")
        self.assertEqual(metadata["request"]["sport_id"], 1)

        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(
            manifest["contract"],
            "mlb-stats-api-schedule-manifest/v1",
        )
        self.assertEqual(
            [game["game_pk"] for game in manifest["games"]],
            [823514, 824726],
        )
        self.assertTrue(
            all(game["processing_status"] == "pending" for game in manifest["games"])
        )

    def test_same_schedule_run_is_idempotent(self) -> None:
        self.store.land(
            schedule=self.schedule,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )
        result = self.store.land(
            schedule=self.schedule,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )

        self.assertFalse(result.created)
        manifest = json.loads(Path(result.manifest_path.uri).read_text())
        self.assertEqual(len(manifest["games"]), 2)

    def test_refuses_different_content_for_the_same_run(self) -> None:
        self.store.land(
            schedule=self.schedule,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )
        changed_payload = json.loads(self.raw)
        changed_payload["copyright"] = "different"
        changed_raw = json.dumps(changed_payload).encode()
        changed_schedule = ScheduleResponse.from_bytes(changed_raw)

        with self.assertRaisesRegex(ScheduleConflictError, "different response"):
            self.store.land(
                schedule=changed_schedule,
                request=self.request(),
                raw=changed_raw,
                run_id=RUN_ID,
            )

    def test_refuses_conflicting_request_for_the_same_run(self) -> None:
        self.store.land(
            schedule=self.schedule,
            request=self.request(),
            raw=self.raw,
            run_id=RUN_ID,
        )

        with self.assertRaisesRegex(ScheduleConflictError, "metadata conflicts"):
            self.store.land(
                schedule=self.schedule,
                request=self.request(sport_id=11),
                raw=self.raw,
                run_id=RUN_ID,
            )
