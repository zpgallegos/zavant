"""Exercise annual-weight eligibility, preflight, and incremental replacement offline."""

import csv
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, NoReturn

import yaml
from jinja2 import Environment, StrictUndefined

from tests.test_batting_outcome_normalization import BattingModelFixture, _render_sql


_DBT = Path(__file__).resolve().parents[1] / "dbt"
_FACT = _DBT / "models/marts/fct_plate_appearances.sql"
_WEIGHT_COLUMNS = (
    "walk_weight",
    "hit_by_pitch_weight",
    "single_weight",
    "double_weight",
    "triple_weight",
    "home_run_weight",
)


def _compiler_error(message: str) -> NoReturn:
    raise ValueError(message)


class WobaMetricTests(BattingModelFixture):
    """Test the production SELECT and YAML, not a parallel Python wOBA formula."""

    def _game(self, game_pk: int, season: int, events: list[str]) -> None:
        self._insert(
            "stg_games",
            {
                "game_pk": game_pk,
                "season": season,
                "official_date": f"{season}-06-01",
                "abstract_game_state": "Final",
                "statsapi_source_revision_id": "stats-1",
            },
        )
        for index, event in enumerate(events):
            self._insert(
                "raw_plays",
                {
                    "game_pk": game_pk,
                    "season": season,
                    "official_date": f"{season}-06-01",
                    "at_bat_index": index,
                    "batter_id": 123,
                    "pitcher_id": 456,
                    "event_type": event,
                    "is_complete": True,
                    "outs": 0,
                    "inning": 1,
                    "half_inning": "top",
                    "is_top_inning": True,
                    "home_score": 0,
                    "away_score": 0,
                },
            )

    def _validate_weights(self) -> None:
        template = (_DBT / "macros/woba.sql").read_text()
        Environment(undefined=StrictUndefined).from_string(
            template + "{{ validate_woba_weights() }}"
        ).render(
            execute=True,
            ref=lambda name: name,
            run_query=lambda sql: SimpleNamespace(
                rows=self.connection.execute(sql).fetchall()
            ),
            exceptions=SimpleNamespace(raise_compiler_error=_compiler_error),
        )

    def _snapshot(self) -> None:
        self.connection.execute(
            "create table existing_plate_appearances as select * from fct_plate_appearances"
        )

    def _incremental_rows(self) -> list[dict[str, Any]]:
        return self._rows(_render_sql(_FACT, incremental=True))

    def _metric(self, where: str = "result_batter_id=123") -> float | None:
        path = _DBT / "models/semantic/plate_appearances"
        metrics = {
            m["name"]: m
            for m in yaml.safe_load(
                (path / "metrics_plate_appearances.yml").read_text()
            )["metrics"]
        }
        measures = {
            m["name"]: m
            for m in yaml.safe_load((path / "sem_plate_appearances.yml").read_text())[
                "semantic_models"
            ][0]["measures"]
        }
        metric = metrics["weighted_on_base_average"]
        self.assertEqual(metric["type"], "ratio")
        expressions = []
        for component in ("numerator", "denominator"):
            simple = metrics[metric["type_params"][component]]
            measure = measures[simple["type_params"]["measure"]]
            self.assertEqual(measure["agg"], "sum")
            expressions.append(f"sum({measure['expr']})")
        numerator, denominator = expressions
        return self.connection.execute(
            f"select 1.0*({numerator})/nullif({denominator},0) "
            f"from fct_plate_appearances where {where}"
        ).fetchone()[0]

    def test_seed_has_unique_seasons_and_explicit_provenance(self) -> None:
        with (_DBT / "seeds/woba_weights.csv").open(newline="") as source:
            rows = list(csv.DictReader(source))
        self.assertEqual([int(r["season"]) for r in rows], list(range(2015, 2027)))
        for row in rows:
            self.assertEqual(
                row["is_provisional"], "true" if row["season"] == "2026" else "false"
            )
            self.assertTrue(row["source_url"].startswith("https://www.fangraphs.com/"))
            self.assertEqual(row["retrieved_on"], "2026-09-27")
            self.assertTrue(
                all(0 < float(row[column]) < 3 for column in _WEIGHT_COLUMNS)
            )

    def test_all_outcomes_use_official_eligibility_without_savant(self) -> None:
        expected = {
            "walk": (0.691, 1),
            "hit_by_pitch": (0.722, 1),
            "single": (0.882, 1),
            "double": (1.252, 1),
            "triple": (1.584, 1),
            "home_run": (2.037, 1),
            "field_out": (0.0, 1),
            "strikeout": (0.0, 1),
            "field_error": (0.0, 1),
            "fielders_choice": (0.0, 1),
            "fielders_choice_out": (0.0, 1),
            "force_out": (0.0, 1),
            "grounded_into_double_play": (0.0, 1),
            "strikeout_double_play": (0.0, 1),
            "sac_fly": (0.0, 1),
            "sac_fly_double_play": (0.0, 1),
            "intent_walk": (0.0, 0),
            "sac_bunt": (0.0, 0),
            "sac_bunt_double_play": (0.0, 0),
            "catcher_interf": (0.0, 0),
        }
        self._game(990001, 2025, list(expected))
        rows = self._rows("select * from fct_plate_appearances where game_pk=990001")
        self.assertEqual(len(rows), len(expected))
        for row in rows:
            with self.subTest(event=row["event_type"]):
                numerator, denominator = expected[row["event_type"]]
                self.assertEqual(row["woba_numerator"], numerator)
                self.assertEqual(row["woba_opportunity_ind"], denominator)
                self.assertIsNone(row["expected_woba_value"])
                self.assertIsNone(row["woba_denominator"])

    def test_each_season_uses_its_own_weights_and_career_is_weighted(self) -> None:
        self._game(990001, 2024, ["home_run", "field_out", "intent_walk"])
        self._game(990002, 2025, ["home_run"])
        rate = self._metric()
        assert rate is not None
        self.assertAlmostEqual(rate, (2.050 + 2.037) / 3)
        self.assertNotAlmostEqual(rate, (2.050 / 2 + 2.037) / 2)

    def test_mookie_2025_reference_rounds_to_savant_display(self) -> None:
        # Retained 2025 official outcomes checked in Athena on 2026-09-27.
        self._game(
            990001,
            2025,
            ["single"] * 107
            + ["double"] * 23
            + ["triple"] * 2
            + ["home_run"] * 20
            + ["field_out"] * 437
            + ["walk"] * 59
            + ["hit_by_pitch"] * 3
            + ["sac_fly"] * 10
            + ["intent_walk"] * 2,
        )
        rate = self._metric()
        assert rate is not None
        self.assertAlmostEqual(rate, 0.3177201210287443)
        self.assertEqual(f"{rate:.3f}", "0.318")

    def test_zero_opportunities_and_empty_population_return_null(self) -> None:
        self._game(990001, 2025, ["intent_walk", "sac_bunt", "catcher_interf"])
        self.assertIsNone(self._metric())
        self.assertIsNone(self._metric("1=0"))

    def test_ordinary_outs_return_zero_not_null(self) -> None:
        self._game(990001, 2025, ["strikeout", "field_out"])
        self.assertEqual(self._metric(), 0.0)

    def test_missing_season_fails_preflight_without_dropping_pa_rows(self) -> None:
        self._game(990001, 2027, ["single", "field_out"])
        with self.assertRaisesRegex(ValueError, "weights for seasons: 2027"):
            self._validate_weights()
        rows = self._rows(
            "select woba_numerator from fct_plate_appearances where game_pk=990001"
        )
        self.assertEqual(rows, [{"woba_numerator": None}, {"woba_numerator": None}])

    def test_duplicate_and_invalid_weights_fail_preflight(self) -> None:
        self._game(990001, 2025, ["single"])
        self._validate_weights()
        self.connection.execute(
            "insert into woba_weights select * from woba_weights where season=2025"
        )
        with self.assertRaisesRegex(ValueError, "2025"):
            self._validate_weights()
        self.connection.execute(
            "delete from woba_weights where rowid=(select max(rowid) from woba_weights)"
        )
        for column in _WEIGHT_COLUMNS:
            with self.subTest(column=column):
                old = self.connection.execute(
                    f"select {column} from woba_weights where season=2025"
                ).fetchone()[0]
                self.connection.execute(
                    f"update woba_weights set {column}=null where season=2025"
                )
                with self.assertRaisesRegex(ValueError, "2025"):
                    self._validate_weights()
                self.connection.execute(
                    f"update woba_weights set {column}=? where season=2025", (old,)
                )

    def test_unchanged_revisions_skip_existing_games(self) -> None:
        self._game(990001, 2025, ["single"])
        self._snapshot()
        self.assertEqual(self._incremental_rows(), [])

    def test_invalid_coefficients_and_missing_provisional_status_fail_preflight(
        self,
    ) -> None:
        self._game(990001, 2025, ["single"])
        for invalid in (0, -1, 99):
            with self.subTest(weight=invalid):
                self.connection.execute(
                    "update woba_weights set single_weight=? where season=2025",
                    (invalid,),
                )
                with self.assertRaisesRegex(ValueError, "2025"):
                    self._validate_weights()
        self.connection.execute(
            "update woba_weights set single_weight=0.882,is_provisional=null where season=2025"
        )
        with self.assertRaisesRegex(ValueError, "2025"):
            self._validate_weights()

    def test_weight_change_replaces_only_affected_season_without_new_source_revision(
        self,
    ) -> None:
        self._game(990001, 2024, ["single"])
        self._game(990002, 2025, ["single", "field_out"])
        self._snapshot()
        self.connection.execute(
            "update woba_weights set single_weight=0.9 where season=2025"
        )
        rows = self._incremental_rows()
        self.assertEqual({r["game_pk"] for r in rows}, {990002})
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(r["woba_numerator"] for r in rows), 0.9)
        self.assertTrue(all(not r["_dbt_is_deleted"] for r in rows))

    def test_metadata_only_retrieval_update_does_not_rebuild(self) -> None:
        self._game(990001, 2025, ["single"])
        self._snapshot()
        self.connection.execute("update woba_weights set retrieved_on='2026-09-28'")
        self.assertEqual(self._incremental_rows(), [])

    def test_finalizing_weights_updates_the_provisional_flag(self) -> None:
        self._game(990001, 2026, ["single"])
        self._snapshot()
        self.connection.execute(
            "update woba_weights set is_provisional=false where season=2026"
        )
        rows = self._incremental_rows()
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["woba_weights_is_provisional"])

    def test_source_correction_and_weight_change_do_not_duplicate_game_rows(
        self,
    ) -> None:
        self._game(990001, 2025, ["single", "field_out"])
        self._snapshot()
        self.connection.execute(
            "update woba_weights set single_weight=0.9 where season=2025"
        )
        self.connection.execute(
            "update stg_games set statsapi_source_revision_id='stats-2' where game_pk=990001"
        )
        self.connection.execute(
            "delete from raw_plays where game_pk=990001 and at_bat_index=1"
        )
        rows = self._incremental_rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(r["_dbt_is_deleted"] for r in rows), 1)

    def test_warehouse_test_detects_stale_weights_and_wrong_components(self) -> None:
        self._game(990001, 2025, ["single"])
        self._snapshot()
        sql = _render_sql(
            _DBT / "tests/plate_appearance_fact_uses_current_woba_weights.sql"
        )
        sql = sql.replace(
            "from fct_plate_appearances", "from existing_plate_appearances"
        )
        self.assertEqual(self._rows(sql), [])
        self.connection.execute(
            "update existing_plate_appearances set woba_numerator=0.9 where game_pk=990001"
        )
        self.assertEqual(self._rows(sql), [{"game_pk": 990001, "at_bat_index": 0}])
        self.connection.execute(
            "update existing_plate_appearances set woba_numerator=0.882 where game_pk=990001"
        )
        self.connection.execute(
            "update woba_weights set single_weight=0.9 where season=2025"
        )
        self.assertEqual(self._rows(sql), [{"game_pk": 990001, "at_bat_index": 0}])


if __name__ == "__main__":
    unittest.main()
