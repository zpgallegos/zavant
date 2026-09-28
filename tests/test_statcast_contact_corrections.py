"""Exercise the actual fact SELECT and semantic YAML against offline contact cases."""

import unittest
from pathlib import Path
from typing import Any

import yaml

from tests.test_batting_outcome_normalization import BattingModelFixture, _render_sql


_DBT = Path(__file__).resolve().parents[1] / "dbt"
_FACT = _DBT / "models/marts/fct_batted_balls.sql"
_CONTACT_TEST = _DBT / "tests/batted_ball_fact_matches_contact_measurements.sql"


class StatcastContactCorrectionsTests(BattingModelFixture):
    """Keep source coverage, rate eligibility, and deferred sweet spots independent."""

    def setUp(self) -> None:
        super().setUp()
        self._table("stg_batted_balls", "stg_batted_balls")
        self._insert(
            "stg_games",
            {
                "game_pk": 990001,
                "season": 2025,
                "official_date": "2025-06-01",
                "abstract_game_state": "Final",
                "statsapi_source_revision_id": "stats-1",
            },
        )
        self._insert(
            "stg_statcast_date_revisions",
            {"game_date": "2025-06-01", "savant_source_revision_id": "savant-1"},
        )
        self.connection.execute(f"create view fct_batted_balls as {_render_sql(_FACT)}")

    def _play(
        self,
        index: int,
        event: str = "field_out",
        **savant_values: Any,
    ) -> None:
        self._insert(
            "raw_plays",
            {
                "game_pk": 990001,
                "season": 2025,
                "official_date": "2025-06-01",
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
        self._insert(
            "stg_statcast_batting_events",
            {
                "game_pk": 990001,
                "at_bat_number": index + 1,
                # Deliberately different: the cross-source join must not use pitch number.
                "pitch_number": 9,
                "event": event,
                **savant_values,
            },
        )

    def _contact(
        self,
        index: int,
        *,
        stats_angle: float | None = None,
        stats_speed: float | None = None,
        savant_angle: float | None = None,
        savant_speed: float | None = None,
        barrel: bool = False,
        bunt: bool = False,
    ) -> None:
        self._play(
            index,
            launch_angle=savant_angle,
            launch_speed=savant_speed,
            launch_speed_angle=6 if barrel else None,
        )
        self._insert(
            "stg_batted_balls",
            {
                "game_pk": 990001,
                "at_bat_index": index,
                "event_index": 2,
                "pitch_number": 3,
                "season": 2025,
                "official_date": "2025-06-01",
                "trajectory": "bunt_grounder" if bunt else "line_drive",
                "launch_angle": stats_angle,
                "launch_speed": stats_speed,
            },
        )

    def _metric(
        self, model: str, name: str, where: str = "game_pk=990001"
    ) -> float | None:
        # Evaluate the checked-in expressions; don't restate metric formulas in Python.
        directory = _DBT / "models/semantic" / model
        metrics = {
            m["name"]: m
            for m in yaml.safe_load((directory / f"metrics_{model}.yml").read_text())[
                "metrics"
            ]
        }
        measures = {
            m["name"]: m
            for m in yaml.safe_load((directory / f"sem_{model}.yml").read_text())[
                "semantic_models"
            ][0]["measures"]
        }

        def component(metric_name: str) -> str:
            definition = metrics[metric_name]
            self.assertEqual(definition["type"], "simple")
            measure = measures[definition["type_params"]["measure"]]
            self.assertIn(measure["agg"], {"sum", "count", "max"})
            return f"{measure['agg']}({measure['expr']})"

        metric = metrics[name]
        if metric["type"] == "ratio":
            numerator = component(metric["type_params"]["numerator"])
            denominator = component(metric["type_params"]["denominator"])
            expression = f"1.0*({numerator})/nullif({denominator},0)"
        else:
            expression = component(name)
        return self.connection.execute(
            f"select {expression} from fct_{model} where {where}"
        ).fetchone()[0]

    def _mixed_coverage(self) -> None:
        self._contact(
            0,
            stats_angle=20,
            stats_speed=100,
            savant_angle=21,
            savant_speed=96,
            barrel=True,
        )
        self._contact(1, savant_angle=20, savant_speed=80)
        self._contact(2, stats_angle=8, stats_speed=80)
        self._contact(3, savant_angle=30, savant_speed=70)
        self._contact(4)

    def test_fact_keeps_both_sources_and_uses_savant_without_fallback(self) -> None:
        self._mixed_coverage()
        rows = self._rows("""
            select at_bat_index, launch_angle, launch_speed,
                   statsapi_launch_angle, statsapi_launch_speed,
                   has_exit_velocity, has_launch_angle, has_statcast_tracking,
                   is_hard_hit, is_sweet_spot
            from fct_batted_balls order by at_bat_index
        """)
        self.assertEqual(len(rows), 5)
        self.assertEqual(
            rows[0],
            {
                "at_bat_index": 0,
                "launch_angle": 21,
                "launch_speed": 96,
                "statsapi_launch_angle": 20,
                "statsapi_launch_speed": 100,
                "has_exit_velocity": 1,
                "has_launch_angle": 1,
                "has_statcast_tracking": 1,
                "is_hard_hit": 1,
                "is_sweet_spot": 1,
            },
        )
        self.assertEqual(rows[1]["has_statcast_tracking"], 1)
        self.assertIsNone(rows[1]["statsapi_launch_angle"])
        self.assertEqual(rows[1]["is_sweet_spot"], 0)
        self.assertIsNone(rows[2]["launch_angle"])
        self.assertIsNone(rows[2]["launch_speed"])
        self.assertEqual(rows[2]["has_statcast_tracking"], 0)
        self.assertEqual(rows[2]["is_sweet_spot"], 1)
        self.assertEqual(self._rows(_render_sql(_CONTACT_TEST)), [])

    def test_contact_averages_include_savant_only_measurements(self) -> None:
        self._mixed_coverage()
        self.assertEqual(self._metric("batted_balls", "average_exit_velocity"), 82)
        angle = self._metric("batted_balls", "average_launch_angle")
        assert angle is not None
        self.assertAlmostEqual(angle, 71 / 3)
        self.assertEqual(self._metric("batted_balls", "maximum_exit_velocity"), 96)

    def test_source_check_detects_a_corrupted_measurement(self) -> None:
        self._mixed_coverage()
        self.connection.execute(
            "create table snapshot as select * from fct_batted_balls"
        )
        self.connection.execute("drop view fct_batted_balls")
        self.connection.execute("alter table snapshot rename to fct_batted_balls")
        self.connection.execute(
            "update fct_batted_balls set launch_angle=999 where at_bat_index=0"
        )
        rows = self._rows(_render_sql(_CONTACT_TEST))
        self.assertEqual([r["at_bat_index"] for r in rows], [0])

    def test_savant_revision_refreshes_contact_without_changing_sweet_spots(
        self,
    ) -> None:
        self._mixed_coverage()
        # The shared renderer's target name is a fixture only, not a warehouse table.
        self.connection.execute(
            "create table existing_plate_appearances as select * from fct_batted_balls"
        )
        self.assertEqual(self._rows(_render_sql(_FACT, incremental=True)), [])
        self.connection.execute(
            "update stg_statcast_date_revisions set savant_source_revision_id='savant-2'"
        )
        self.connection.execute(
            "update stg_statcast_batting_events set launch_angle=40, launch_speed=110 "
            "where game_pk=990001 and at_bat_number=1"
        )
        rows = self._rows(_render_sql(_FACT, incremental=True))
        self.assertEqual(len(rows), 5)
        row = next(r for r in rows if r["at_bat_index"] == 0)
        self.assertEqual(row["launch_angle"], 40)
        self.assertEqual(row["launch_speed"], 110)
        self.assertEqual(row["savant_source_revision_id"], "savant-2")
        self.assertEqual(row["statsapi_launch_angle"], 20)
        self.assertEqual(row["sweet_spot_ind"], 1)

    def test_hard_hit_boundary_uses_savant_not_original_velocity(self) -> None:
        self._contact(0, stats_speed=110, savant_speed=94.9)
        self._contact(1, stats_speed=80, savant_speed=95)
        self._contact(2, savant_speed=100)
        self._contact(3)
        self.assertEqual(self._metric("batted_balls", "hard_hits"), 2)
        self.assertEqual(self._metric("batted_balls", "hard_hit_rate"), 0.5)

    def test_barrels_hard_hits_and_sweet_spots_use_different_populations(self) -> None:
        self._mixed_coverage()
        self.assertEqual(self._metric("batted_balls", "barrel_rate"), 1 / 2)
        self.assertEqual(self._metric("batted_balls", "hard_hit_rate"), 1 / 5)
        self.assertEqual(self._metric("batted_balls", "sweet_spot_rate"), 2 / 2)
        self.assertEqual(self._metric("batted_balls", "statcast_tracking_rate"), 3 / 5)

    def test_sweet_spot_source_boundaries_and_missing_values_do_not_change(
        self,
    ) -> None:
        for index, angle in enumerate((7, 8, 32, 33, None)):
            self._contact(index, stats_angle=angle, savant_angle=20, savant_speed=90)
        self.assertEqual(self._metric("batted_balls", "sweet_spots"), 2)
        self.assertEqual(self._metric("batted_balls", "sweet_spot_rate"), 2 / 4)
        self.assertEqual(self._metric("batted_balls", "average_launch_angle"), 20)

    def test_nonbunt_angle_average_still_excludes_bunts(self) -> None:
        self._contact(0, stats_angle=10, savant_angle=20)
        self._contact(1, stats_angle=50, savant_angle=60, bunt=True)
        self.assertEqual(self._metric("batted_balls", "average_launch_angle"), 20)

    def test_null_and_zero_contact_observations_are_distinct(self) -> None:
        self._contact(0)
        for metric in (
            "barrel_rate",
            "average_exit_velocity",
            "average_launch_angle",
            "sweet_spot_rate",
        ):
            with self.subTest(metric=metric):
                self.assertIsNone(self._metric("batted_balls", metric))
                self.assertIsNone(self._metric("batted_balls", metric, "1=0"))
        self.assertEqual(self._metric("batted_balls", "hard_hit_rate"), 0)
        self.assertIsNone(self._metric("batted_balls", "hard_hit_rate", "1=0"))
        self._contact(1, stats_angle=0, stats_speed=0, savant_angle=0, savant_speed=0)
        self.assertEqual(self._metric("batted_balls", "average_exit_velocity"), 0)
        self.assertEqual(self._metric("batted_balls", "average_launch_angle"), 0)
        self.assertEqual(self._metric("batted_balls", "barrel_rate"), 0)

    def test_missing_savant_join_preserves_bbe_and_original_sweet_spot(self) -> None:
        self._contact(
            0, stats_angle=20, stats_speed=100, savant_angle=20, savant_speed=100
        )
        self.connection.execute(
            "delete from stg_statcast_batting_events where game_pk=990001"
        )
        self.assertEqual(self._metric("batted_balls", "batted_ball_events"), 1)
        self.assertIsNone(self._metric("batted_balls", "average_exit_velocity"))
        self.assertEqual(self._metric("batted_balls", "sweet_spot_rate"), 1)
        self.assertEqual(self._rows(_render_sql(_CONTACT_TEST)), [])

    def test_missing_contact_is_not_a_strikeout_or_a_zero_estimate(self) -> None:
        for index, (event, ba, slg) in enumerate(
            (
                ("single", 0.6, 1.2),
                ("field_out", 0.0, 0.0),
                ("strikeout", None, None),
                ("field_out", None, None),
                # Even if provided, estimates on a walk/SF do not enter xBA/xSLG.
                ("walk", 0.9, 3.0),
                ("sac_fly", 0.8, 2.0),
            )
        ):
            self._play(
                index,
                event,
                estimated_ba_using_speedangle=ba,
                estimated_slg_using_speedangle=slg,
            )
        self.assertEqual(self._metric("plate_appearances", "at_bats"), 4)
        self.assertEqual(
            self._metric("plate_appearances", "expected_batting_average_at_bats"), 3
        )
        self.assertEqual(
            self._metric("plate_appearances", "expected_slugging_at_bats"), 3
        )
        self.assertAlmostEqual(
            self._metric("plate_appearances", "expected_batting_average") or 0, 0.2
        )
        self.assertAlmostEqual(
            self._metric("plate_appearances", "expected_slugging_percentage") or 0, 0.4
        )

    def test_xba_and_xslg_eligibility_are_independent(self) -> None:
        self._play(0, "single", estimated_ba_using_speedangle=0.5)
        self._play(1, "double", estimated_slg_using_speedangle=1.2)
        self._play(2, "strikeout")
        self.assertEqual(
            self._metric("plate_appearances", "expected_batting_average"), 0.25
        )
        self.assertEqual(
            self._metric("plate_appearances", "expected_slugging_percentage"), 0.6
        )

    def test_expected_rates_return_null_without_eligible_at_bats(self) -> None:
        self._play(0, "field_out")
        self._play(
            1,
            "walk",
            estimated_ba_using_speedangle=1.0,
            estimated_slg_using_speedangle=4.0,
        )
        for metric in ("expected_batting_average", "expected_slugging_percentage"):
            with self.subTest(metric=metric):
                self.assertIsNone(self._metric("plate_appearances", metric))
                self.assertIsNone(self._metric("plate_appearances", metric, "1=0"))
        self._play(2, "strikeout_double_play")
        self.assertEqual(
            self._metric("plate_appearances", "expected_batting_average"), 0
        )
        self.assertEqual(
            self._metric("plate_appearances", "expected_slugging_percentage"), 0
        )

    def test_mookie_2020_expected_rate_denominators(self) -> None:
        # Retained totals: 219 AB, two unestimated contacts, 61.507 xH, 105.261 xTB.
        # Distribute sums over the covered contacts to test the actual YAML ratios.
        for index in range(219):
            if index < 179:
                self._play(
                    index,
                    estimated_ba_using_speedangle=61.507 / 179,
                    estimated_slg_using_speedangle=105.261 / 179,
                )
            else:
                self._play(index, "strikeout" if index < 217 else "field_out")
        xba = self._metric("plate_appearances", "expected_batting_average")
        xslg = self._metric("plate_appearances", "expected_slugging_percentage")
        assert xba is not None and xslg is not None
        self.assertEqual(f"{xba:.3f}", "0.283")
        self.assertEqual(f"{xslg:.3f}", "0.485")

    def test_expected_rates_weight_regrouped_opportunities(self) -> None:
        self._play(
            0,
            "single",
            estimated_ba_using_speedangle=0.6,
            estimated_slg_using_speedangle=1.2,
        )
        self._play(1, "strikeout")
        self._play(2, "strikeout")
        self._play(3, "field_out")
        xba = self._metric("plate_appearances", "expected_batting_average")
        assert xba is not None
        self.assertAlmostEqual(xba, 0.6 / 3)
        self.assertNotAlmostEqual(xba, (0.6 + 0.0) / 2)


if __name__ == "__main__":
    unittest.main()
