"""Run the actual dbt SELECTs against small, offline batting-anomaly fixtures.

SQLite covers the relational behavior without Athena writes or another database
dependency. dbt's materialization and surrogate-key hash are not under test;
Athena dialect compatibility is checked separately with SQLFluff/read-only SQL.
"""

import json
import sqlite3
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml
from jinja2 import Environment, StrictUndefined


_ROOT = Path(__file__).resolve().parents[1]
_DBT = _ROOT / "dbt"
_MODEL_PATHS = {path.stem: path for path in (_DBT / "models").rglob("*.sql")}


def _fixture_key(columns: list[str]) -> str:
    return " || ':' || ".join(f"cast({column} as text)" for column in columns)


def _render_sql(path: Path) -> str:
    macros = "\n".join(
        macro.read_text(encoding="utf-8")
        for macro in sorted((_DBT / "macros").glob("*.sql"))
    )
    return (
        Environment(undefined=StrictUndefined)
        .from_string(macros + path.read_text(encoding="utf-8"))
        .render(
            ref=lambda name: name,
            source=lambda schema, name: f"raw_{name}",
            config=lambda **kwargs: "",
            is_incremental=lambda: False,
            dbt_utils=SimpleNamespace(generate_surrogate_key=_fixture_key),
        )
    )


class BattingOutcomeNormalizationTests(unittest.TestCase):
    def _table(self, name: str, model: str) -> None:
        # Model documentation supplies all columns, including unused nullable
        # fields, so fixtures can focus on the evidence behind each correction.
        schema = yaml.safe_load(_MODEL_PATHS[model].with_suffix(".yml").read_text())
        columns = schema["models"][0]["columns"]
        declarations = []
        for column in columns:
            data_type = column.get("data_type", "varchar")
            sqlite_type = (
                "integer"
                if data_type in {"bigint", "integer", "boolean"}
                else "real"
                if data_type in {"double", "float"}
                else "text"
            )
            declarations.append(f"{column['name']} {sqlite_type}")
        self.connection.execute(f"create table {name} ({', '.join(declarations)})")

    def _insert(self, table: str, row: dict[str, Any]) -> None:
        columns = ", ".join(row)
        placeholders = ", ".join("?" for _ in row)
        self.connection.execute(
            f"insert into {table} ({columns}) values ({placeholders})",
            tuple(row.values()),
        )

    def _rows(self, sql: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(sql)]

    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.row_factory = sqlite3.Row
        self.connection.create_function("greatest", -1, max)
        # These Athena scalar functions are immaterial to the tested grain/rules.
        self.connection.create_function(
            "if", 3, lambda value, yes, no: yes if value else no
        )
        self.connection.create_function(
            "concat", -1, lambda *values: "".join(map(str, values))
        )

        fixture = json.loads(
            (_ROOT / "tests/fixtures/batting-outcome-anomalies.json").read_text()
        )
        tables = {
            "raw_plays": "stg_plays",
            "raw_player_batting": "stg_boxscore_player_batting",
            "raw_team_batting": "stg_boxscore_team_batting",
            "stg_games": "stg_games",
            "stg_runner_movements": "stg_runner_movements",
            "stg_fielding_credits": "stg_fielding_credits",
            "stg_pitches": "stg_pitches",
            "stg_statcast_batting_events": "stg_statcast_batting_events",
            "stg_statcast_date_revisions": "stg_statcast_date_revisions",
        }
        for table, model in tables.items():
            self._table(table, model)
        for category, table in (
            ("games", "stg_games"),
            ("runner_movements", "stg_runner_movements"),
            ("fielding_credits", "stg_fielding_credits"),
            ("player_batting", "raw_player_batting"),
            ("team_batting", "raw_team_batting"),
        ):
            for row in fixture[category]:
                self._insert(table, row)
        for row in fixture["plays"]:
            game = next(g for g in fixture["games"] if g["game_pk"] == row["game_pk"])
            self._insert(
                "raw_plays",
                {
                    "is_complete": True,
                    "has_review": False,
                    "is_out": True,
                    "outs": 0,
                    "inning": 1,
                    "half_inning": "bottom",
                    "home_score": 0,
                    "away_score": 0,
                    "rbi": 0,
                    "season": game["season"],
                    "official_date": game["official_date"],
                    **row,
                },
            )
        for model in (
            "stg_plays",
            "stg_boxscore_player_batting",
            "stg_boxscore_team_batting",
            "int_plate_appearances",
            "int_at_bats",
            "fct_plate_appearances",
        ):
            self.connection.execute(
                f"create view {model} as {_render_sql(_MODEL_PATHS[model])}"
            )

    def test_all_three_batters_reconcile_after_normalization(self) -> None:
        rows = self._rows("""
            select f.game_pk, count(*) as pa, sum(f.at_bat_ind) as ab,
                   b.plate_appearances as box_pa, b.at_bats as box_ab
            from fct_plate_appearances f
            join stg_boxscore_player_batting b
              on f.game_pk=b.game_pk and f.result_batter_id=b.player_id
            group by f.game_pk, b.plate_appearances, b.at_bats
            order by f.game_pk
        """)
        self.assertEqual(
            rows,
            [
                {"game_pk": 448816, "pa": 5, "ab": 5, "box_pa": 5, "box_ab": 5},
                {"game_pk": 490101, "pa": 5, "ab": 4, "box_pa": 5, "box_ab": 4},
                {"game_pk": 490136, "pa": 4, "ab": 4, "box_pa": 4, "box_ab": 4},
            ],
        )

    def test_other_runner_interference_does_not_change_batting_outcome(self) -> None:
        row = self._rows("""
            select is_at_bat, is_reached_on_error, is_interference, outcome_group
            from fct_plate_appearances where game_pk=448816 and at_bat_index=26
        """)[0]
        self.assertEqual(
            row,
            {
                "is_at_bat": 1,
                "is_reached_on_error": 1,
                "is_interference": 0,
                "outcome_group": "reached_on_error",
            },
        )

    def test_actual_batter_first_base_awards_still_exclude_at_bats(self) -> None:
        for credit, outcome in (
            ("f_interference", "interference"),
            ("f_defensive_shift_violation_error", "defensive_shift_violation"),
        ):
            with self.subTest(credit=credit):
                self.connection.execute(
                    "update stg_fielding_credits set runner_index=1, credit=? "
                    "where game_pk=448816 and credit != 'f_fielding_error'",
                    (credit,),
                )
                rows = self._rows("""
                    select at_bat_ind, outcome_group from fct_plate_appearances
                    where game_pk=448816 and at_bat_index=26
                """)
                self.assertEqual(rows, [{"at_bat_ind": 0, "outcome_group": outcome}])

    def test_interference_on_batter_advancing_beyond_first_is_still_at_bat(
        self,
    ) -> None:
        self.connection.execute(
            "update stg_fielding_credits set runner_index=2 where credit='f_interference'"
        )
        self.assertEqual(
            self._rows("""
            select at_bat_ind, is_interference from fct_plate_appearances
            where game_pk=448816 and at_bat_index=26
        """),
            [{"at_bat_ind": 1, "is_interference": 0}],
        )

    def test_duplicate_award_credits_do_not_duplicate_plate_appearances(self) -> None:
        for index in (1, 2):
            self._insert(
                "stg_fielding_credits",
                {
                    "game_pk": 448816,
                    "at_bat_index": 26,
                    "runner_index": 1,
                    "credit_index": index,
                    "credit": "f_interference",
                },
            )
        self.assertEqual(
            self._rows("""
            select at_bat_ind from fct_plate_appearances
            where game_pk=448816 and at_bat_index=26
        """),
            [{"at_bat_ind": 0}],
        )

    def test_normal_non_at_bat_outcomes_remain_excluded(self) -> None:
        for index, event in enumerate(
            (
                "walk",
                "intent_walk",
                "hit_by_pitch",
                "catcher_interf",
                "sac_bunt",
                "sac_bunt_double_play",
                "sac_fly",
                "sac_fly_double_play",
            ),
            start=100,
        ):
            self._insert(
                "raw_plays",
                {
                    "game_pk": 448816,
                    "at_bat_index": index,
                    "event_type": event,
                },
            )
        self.assertEqual(
            self._rows(
                "select at_bat_index from int_at_bats where at_bat_index >= 100"
            ),
            [],
        )
        self.assertEqual(
            len(
                self._rows(
                    "select at_bat_index from int_plate_appearances where at_bat_index >= 100"
                )
            ),
            8,
        )

    def test_recovered_outs_preserve_reported_values(self) -> None:
        rows = self._rows("""
            select game_pk, event_type, reported_event_type, is_complete,
                   reported_is_complete, is_out, reported_is_out
            from stg_plays where is_batting_outcome_recovered order by game_pk
        """)
        self.assertEqual(
            rows,
            [
                {
                    "game_pk": game,
                    "event_type": event,
                    "reported_event_type": "game_advisory",
                    "is_complete": 1,
                    "reported_is_complete": 0,
                    "is_out": 1,
                    "reported_is_out": 0,
                }
                for game, event in (
                    (490101, "grounded_into_double_play"),
                    (490136, "field_out"),
                )
            ],
        )
        self.assertEqual(
            self._rows("""
            select outcome_group, at_bat_ind, is_out, is_complete
            from fct_plate_appearances where game_pk=490136 and at_bat_index=72
        """),
            [{"outcome_group": "out", "at_bat_ind": 1, "is_out": 1, "is_complete": 1}],
        )

    def test_unsafe_advisories_are_not_recovered(self) -> None:
        # Each mutation removes a separate piece of required evidence.
        mutations = (
            "update stg_games set abstract_game_state='Live' where game_pk=490136",
            "update raw_plays set has_review=false where game_pk=490136",
            "delete from stg_runner_movements where game_pk=490136",
            "update stg_runner_movements set runner_id=999 where game_pk=490136",
            "update stg_runner_movements set start_base='1B' where game_pk=490136",
            "update stg_runner_movements set is_out=false where game_pk=490136",
            "update stg_runner_movements set event_type='pickoff_1b' where game_pk=490136",
            "insert into stg_runner_movements select * from stg_runner_movements where game_pk=490136",
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.connection.execute("savepoint scenario")
                self.connection.execute(mutation)
                self.assertEqual(
                    self._rows("""
                    select event_type, is_batting_outcome_recovered from stg_plays
                    where game_pk=490136 and at_bat_index=72
                """),
                    [
                        {
                            "event_type": "game_advisory",
                            "is_batting_outcome_recovered": 0,
                        }
                    ],
                )
                self.assertEqual(
                    self._rows("""
                    select * from int_plate_appearances
                    where game_pk=490136 and at_bat_index=72
                """),
                    [],
                )
                self.connection.execute("rollback to scenario")
                self.connection.execute("release scenario")

    def test_boxscore_normalization_is_auditable_at_player_and_team_grains(
        self,
    ) -> None:
        for model, original, corrected in (
            ("stg_boxscore_player_batting", [5, 4, 3], [5, 5, 4]),
            ("stg_boxscore_team_batting", [44, 38, 34], [44, 39, 35]),
        ):
            with self.subTest(model=model):
                rows = self._rows(f"select * from {model} order by game_pk")
                self.assertEqual(
                    [r["reported_plate_appearances"] for r in rows], original
                )
                self.assertEqual([r["plate_appearances"] for r in rows], corrected)
                self.assertEqual(
                    [r["plate_appearances_correction_reason"] for r in rows],
                    [
                        None,
                        "recovered_reviewed_batter_out",
                        "recovered_reviewed_batter_out",
                    ],
                )

    def test_correct_pa_unknown_shortfalls_and_null_components_are_not_rewritten(
        self,
    ) -> None:
        for assignment, expected in (
            ("plate_appearances=4", 4),
            ("plate_appearances=2", 2),
            ("base_on_balls=null", 3),
        ):
            with self.subTest(assignment=assignment):
                self.connection.execute("savepoint scenario")
                self.connection.execute(
                    f"update raw_player_batting set {assignment} where game_pk=490136"
                )
                row = self._rows(
                    "select * from stg_boxscore_player_batting where game_pk=490136"
                )[0]
                self.assertEqual(row["plate_appearances"], expected)
                self.assertIsNone(row["plate_appearances_correction_reason"])
                self.connection.execute("rollback to scenario")
                self.connection.execute("release scenario")

    def test_corrections_do_not_leak_to_another_player_or_team(self) -> None:
        row = self._rows("select * from raw_player_batting where game_pk=490136")[0]
        self._insert("raw_player_batting", {**row, "player_id": 999})
        self._insert("raw_player_batting", {**row, "team_id": 999})
        rows = self._rows("""
            select plate_appearances, plate_appearances_correction_reason
            from stg_boxscore_player_batting where player_id=999 or team_id=999
        """)
        self.assertEqual(
            rows,
            [
                {"plate_appearances": 3, "plate_appearances_correction_reason": None},
                {"plate_appearances": 3, "plate_appearances_correction_reason": None},
            ],
        )

    def test_unknown_boxscore_inconsistencies_still_fail_the_data_test(self) -> None:
        sql = _render_sql(
            _DBT / "tests/boxscore_batting_plate_appearances_are_consistent.sql"
        )
        self.assertEqual(self._rows(sql), [])
        self.connection.execute(
            "update raw_player_batting set plate_appearances=2 where game_pk=490136"
        )
        rows = self._rows(sql)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["game_pk"], 490136)
        self.assertEqual(rows[0]["line_type"], "player")

    def test_multiple_recovered_outs_are_counted_not_assumed_to_be_one(self) -> None:
        play = self._rows(
            "select * from raw_plays where game_pk=490136 and at_bat_index=72"
        )[0]
        movement = self._rows(
            "select * from stg_runner_movements where game_pk=490136"
        )[0]
        self._insert("raw_plays", {**play, "at_bat_index": 73})
        self._insert("stg_runner_movements", {**movement, "at_bat_index": 73})
        for table in ("raw_player_batting", "raw_team_batting"):
            self.connection.execute(
                f"update {table} set at_bats=at_bats+1 where game_pk=490136"
            )
        self.assertEqual(
            self._rows("""
            select reported_plate_appearances, plate_appearances
            from stg_boxscore_player_batting where game_pk=490136
        """),
            [{"reported_plate_appearances": 3, "plate_appearances": 5}],
        )
        self.assertEqual(
            self._rows("""
            select reported_plate_appearances, plate_appearances
            from stg_boxscore_team_batting where game_pk=490136
        """),
            [{"reported_plate_appearances": 34, "plate_appearances": 36}],
        )

    def test_intentional_walks_are_not_counted_twice(self) -> None:
        self.connection.execute(
            "update raw_player_batting set intentional_walks=1 where game_pk=490101"
        )
        self.assertEqual(
            self._rows("""
            select reported_plate_appearances, plate_appearances
            from stg_boxscore_player_batting where game_pk=490101
        """),
            [{"reported_plate_appearances": 4, "plate_appearances": 5}],
        )
