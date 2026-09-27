"""Exercise contact-metric YAML expressions without querying the warehouse."""

import sqlite3
import unittest
from pathlib import Path
from typing import Any

import yaml


_ROOT = Path(__file__).resolve().parents[1]
_SEMANTIC = _ROOT / "dbt/models/semantic"
_CONTACT_METRIC = "expected_weighted_on_base_average_on_contact"


def _definitions(path: Path, key: str) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {definition["name"]: definition for definition in document[key]}


class StatcastMetricTests(unittest.TestCase):
    """Keep contact-only eligibility and weighted regrouping in the metric contract."""

    def setUp(self) -> None:
        self.metrics = _definitions(
            _SEMANTIC / "batted_balls/metrics_batted_balls.yml", "metrics"
        )
        semantic = _definitions(
            _SEMANTIC / "batted_balls/sem_batted_balls.yml", "semantic_models"
        )["batted_balls"]
        self.assertEqual(semantic["model"], "ref('fct_batted_balls')")
        self.measures = {measure["name"]: measure for measure in semantic["measures"]}
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.executescript("""
            create table contact (
                season integer,
                event_type text,
                expected_weighted_on_base_average real
            );
            insert into contact values
                (2024, 'single', 0.4),
                (2024, 'field_out', 0.2),
                (2024, 'field_out', 0.0),
                (2024, 'sac_fly', 0.6),
                (2024, 'sac_bunt', 0.3),
                (2024, 'field_out', null),
                (2025, 'home_run', 1.0);
        """)

    def _component_sql(self, component: str) -> str:
        metric = self.metrics[component]
        self.assertEqual(metric["type"], "simple")
        measure = self.measures[metric["type_params"]["measure"]]
        self.assertIn(measure["agg"], {"sum", "count"})
        return f"{measure['agg']}({measure['expr']})"

    def _rate(self, where: str = "1=1") -> float | None:
        metric = self.metrics[_CONTACT_METRIC]
        self.assertEqual(metric["type"], "ratio")
        params = metric["type_params"]
        numerator = self._component_sql(params["numerator"])
        denominator = self._component_sql(params["denominator"])
        return self.connection.execute(
            f"select 1.0 * ({numerator}) / nullif({denominator}, 0) "
            f"from contact where {where}"
        ).fetchone()[0]

    def test_contact_rate_excludes_missing_estimates_but_keeps_zero(self) -> None:
        rate = self._rate("season=2024")
        assert rate is not None
        self.assertAlmostEqual(rate, 1.5 / 5)

    def test_home_runs_are_contact(self) -> None:
        self.assertEqual(self._rate("event_type='home_run'"), 1.0)

    def test_sacrifices_with_estimates_remain_contact(self) -> None:
        rate = self._rate("event_type in ('sac_fly', 'sac_bunt')")
        assert rate is not None
        self.assertAlmostEqual(rate, 0.45)

    def test_zero_is_not_missing(self) -> None:
        self.assertEqual(self._rate("expected_weighted_on_base_average=0"), 0.0)

    def test_missing_or_empty_contact_returns_null(self) -> None:
        self.assertIsNone(self._rate("expected_weighted_on_base_average is null"))
        self.assertIsNone(self._rate("season=1900"))

    def test_career_rate_uses_aggregate_components_not_average_season_rates(self) -> None:
        rate = self._rate()
        assert rate is not None
        self.assertAlmostEqual(rate, 2.5 / 6)
        self.assertNotAlmostEqual(rate, (0.3 + 1.0) / 2)

    def test_existing_statcast_columns_remain_available(self) -> None:
        pa_metrics = _definitions(
            _SEMANTIC / "plate_appearances/metrics_plate_appearances.yml", "metrics"
        )
        expected = {
            "expected_batting_average",
            "expected_slugging_percentage",
            "expected_weighted_on_base_average",
            "strikeout_rate",
            "walk_rate",
        }
        self.assertTrue(expected <= pa_metrics.keys())
        self.assertIn("hard_hit_rate", self.metrics)


if __name__ == "__main__":
    unittest.main()
