from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from uuid import UUID

import pyarrow.parquet as pq

from zavant.contracts.raw_game import RawGameResponse
from zavant.projection.contracts import (
    PROJECTION_CONTRACT_VERSION,
    ProjectionContractError,
    TABLE_CONTRACTS,
)
from zavant.projection.local import current_projection_sources, run_local_projection
from zavant.projection.models import ProjectionSource
from zavant.projection.projector import project_game
from zavant.storage._path_io import canonical_json_sha256
from zavant.storage.path_raw import PathRawGameStore


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_GAME = REPOSITORY_ROOT / "tests" / "fixtures" / "example-game-raw.json"
OBSERVED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
PROJECTED_AT = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
RUN_ID = UUID("00000000-0000-0000-0000-000000000015")


class GameProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = SAMPLE_GAME.read_bytes()
        self.game = RawGameResponse.from_bytes(self.raw)
        self.revision_id = canonical_json_sha256(self.game.payload)
        self.source = ProjectionSource(
            game=self.game,
            revision_id=self.revision_id,
            observed_at=OBSERVED_AT,
            source_uri="fixture://example-game",
            raw_object_uri="file:///example-game.json",
        )

    def test_projects_all_explicit_analytical_grains(self) -> None:
        projection = project_game(self.source, RUN_ID, PROJECTED_AT)

        self.assertEqual(len(projection.games), 1)
        self.assertEqual(len(projection.plays), 80)
        self.assertEqual(len(projection.play_events), 408)
        self.assertEqual(len(projection.pitches), 351)
        self.assertEqual(len(projection.batted_balls), 57)
        self.assertEqual(
            {name: len(rows) for name, rows in projection.tables().items()},
            {
                "actions": 46,
                "batted_balls": 57,
                "disengagements": 11,
                "fielding_credits": 93,
                "game_decisions": 3,
                "game_officials": 4,
                "game_teams": 2,
                "games": 1,
                "innings": 9,
                "non_pitch_calls": 0,
                "pitches": 351,
                "play_events": 408,
                "player_batting": 21,
                "player_fielding": 32,
                "player_pitching": 11,
                "player_positions": 35,
                "players": 52,
                "plays": 80,
                "reviews": 1,
                "rule_violations": 0,
                "runner_movements": 109,
                "substitutions": 15,
                "team_batting": 2,
                "team_fielding": 2,
                "team_pitching": 2,
            },
        )
        self.assertEqual(
            projection.event_kind_counts,
            {"action": 46, "pickoff": 9, "pitch": 351, "stepoff": 2},
        )

        game = projection.games[0]
        self.assertEqual(game["game_pk"], 744863)
        self.assertEqual(game["away_team_id"], 119)
        self.assertEqual(game["home_team_id"], 120)
        self.assertEqual(
            game["projection_contract_version"], PROJECTION_CONTRACT_VERSION
        )
        self.assertEqual(game["source_revision_id"], self.revision_id)

        first_play = projection.plays[0]
        self.assertEqual(first_play["at_bat_index"], 0)
        self.assertEqual(first_play["offense_team_id"], 119)
        self.assertEqual(first_play["defense_team_id"], 120)

        first_pitch = projection.pitches[0]
        self.assertEqual(first_pitch["pitch_type_code"], "SI")
        self.assertEqual(first_pitch["call_code"], "B")
        self.assertEqual(first_pitch["start_speed"], 92.1)

        first_batted_ball = projection.batted_balls[0]
        self.assertEqual(first_batted_ball["launch_speed"], 103.0)
        self.assertEqual(first_batted_ball["trajectory"], "line_drive")

        first_runner = projection.tables()["runner_movements"][0]
        self.assertEqual(first_runner["runner_id"], 605141)
        first_team = projection.tables()["game_teams"][0]
        self.assertEqual(first_team["team_side"], "away")
        self.assertEqual(first_team["team_id"], 119)
        player_ids = {row["player_id"] for row in projection.tables()["players"]}
        self.assertIn(605141, player_ids)

    def test_preserves_unknown_event_family_in_event_spine(self) -> None:
        payload = json.loads(self.raw)
        payload["liveData"]["plays"]["allPlays"][0]["playEvents"][0][
            "type"
        ] = "future_event"
        game = RawGameResponse.from_bytes(json.dumps(payload).encode())
        source = ProjectionSource(
            game=game,
            revision_id=canonical_json_sha256(game.payload),
            observed_at=OBSERVED_AT,
            source_uri="fixture://future-event",
            raw_object_uri="file:///future-event.json",
        )

        projection = project_game(source, RUN_ID, PROJECTED_AT)

        self.assertEqual(projection.event_kind_counts["future_event"], 1)
        self.assertEqual(projection.play_events[0]["event_kind"], "future_event")

    def test_projects_player_who_participated_for_both_teams(self) -> None:
        payload = json.loads(self.raw)
        boxscore_teams = payload["liveData"]["boxscore"]["teams"]
        participating_player = boxscore_teams["away"]["players"]["ID669257"]
        second_team_player = json.loads(json.dumps(participating_player))
        second_team_player["parentTeamId"] = 120
        second_team_player["battingOrder"] = "900"
        second_team_player["stats"]["batting"]["hits"] = 2
        boxscore_teams["home"]["players"]["ID669257"] = second_team_player
        game = RawGameResponse.from_bytes(json.dumps(payload).encode())
        source = ProjectionSource(
            game=game,
            revision_id=canonical_json_sha256(game.payload),
            observed_at=OBSERVED_AT,
            source_uri="fixture://dual-team-player",
            raw_object_uri="file:///dual-team-player.json",
        )

        projection = project_game(source, RUN_ID, PROJECTED_AT)

        players = [
            row for row in projection.tables()["players"] if row["player_id"] == 669257
        ]
        batting = [
            row
            for row in projection.tables()["player_batting"]
            if row["player_id"] == 669257
        ]
        self.assertEqual(
            {(row["team_id"], row["team_side"]) for row in players},
            {(119, "away"), (120, "home")},
        )
        self.assertEqual(
            {(row["team_id"], row["hits"]) for row in batting},
            {(119, 0), (120, 2)},
        )

    def test_rejects_duplicate_event_natural_key(self) -> None:
        projection = project_game(self.source, RUN_ID, PROJECTED_AT)
        duplicate = projection.play_events[0]

        with self.assertRaisesRegex(ProjectionContractError, "duplicate primary key"):
            TABLE_CONTRACTS["play_events"].validate((duplicate, duplicate))


class LocalProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.data_dir = self.root / "lake"
        self.raw = SAMPLE_GAME.read_bytes()
        self.game = RawGameResponse.from_bytes(self.raw)

    def test_discovers_only_revision_selected_by_current_pointer(self) -> None:
        store = PathRawGameStore(self.data_dir, clock=lambda: OBSERVED_AT)
        first = store.land(self.game, self.raw, "fixture://original")
        changed_payload = json.loads(self.raw)
        changed_payload["gameData"]["status"]["detailedState"] = "Final: Corrected"
        changed_raw = json.dumps(changed_payload).encode()
        changed_game = RawGameResponse.from_bytes(changed_raw)
        second = store.land(changed_game, changed_raw, "fixture://corrected")

        sources = list(current_projection_sources(self.data_dir))

        self.assertNotEqual(first.revision_id, second.revision_id)
        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0].revision_id, second.revision_id)
        self.assertEqual(
            sources[0].game.payload["gameData"]["status"]["detailedState"],
            "Final: Corrected",
        )

    def test_writes_parquet_schemas_samples_and_manifest(self) -> None:
        PathRawGameStore(self.data_dir, clock=lambda: OBSERVED_AT).land(
            self.game,
            self.raw,
            "fixture://example-game",
        )
        output_dir = self.root / "projection"

        result = run_local_projection(
            data_dir=self.data_dir,
            output_dir=output_dir,
            run_id=RUN_ID,
            projected_at=PROJECTED_AT,
        )

        self.assertEqual(result.game_count, 1)
        self.assertEqual(result.ignored_unversioned_game_count, 0)
        self.assertEqual(result.row_counts["pitches"], 351)
        manifest = json.loads(result.manifest_path.read_text())
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(manifest["game_count"], 1)
        self.assertEqual(manifest["ignored_unversioned_game_files"], [])
        self.assertEqual(manifest["row_counts"]["batted_balls"], 57)
        self.assertEqual(manifest["row_counts"]["runner_movements"], 109)
        self.assertEqual(manifest["row_counts"]["players"], 52)
        self.assertTrue((output_dir / "schemas.json").exists())
        self.assertTrue((output_dir / "samples" / "pitches.json").exists())
        self.assertTrue((output_dir / "samples" / "players.json").exists())

        pitch_table = pq.read_table(output_dir / "pitches" / "data.parquet")
        self.assertEqual(pitch_table.num_rows, 351)
        self.assertEqual(
            pitch_table.schema.names,
            [column.name for column in TABLE_CONTRACTS["pitches"].columns],
        )
        violation_table = pq.read_table(
            output_dir / "rule_violations" / "data.parquet"
        )
        self.assertEqual(violation_table.num_rows, 0)

    def test_rejects_existing_output_without_overwriting_it(self) -> None:
        output_dir = self.root / "projection"
        output_dir.mkdir()
        marker = output_dir / "owned-by-user.txt"
        marker.write_text("preserve")

        with self.assertRaisesRegex(FileExistsError, "already exists"):
            run_local_projection(self.data_dir, output_dir)

        self.assertEqual(marker.read_text(), "preserve")


if __name__ == "__main__":
    unittest.main()
