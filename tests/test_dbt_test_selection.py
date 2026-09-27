"""Exercise dbt's real test selector without builds or warehouse connections."""

import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml


_ROOT = Path(__file__).resolve().parents[1]
_SAVANT_MODELS = "stg_statcast_batting_events stg_statcast_date_revisions"
_COMBINED_MODELS = "fct_batted_balls fct_plate_appearances"
_COMBINED_TESTS = {
    "batted_ball_fact_matches_statcast_barrel_classification",
    "batted_ball_fact_matches_statcast_expected_statistics",
    "batted_ball_fact_uses_current_savant_revision",
    "plate_appearance_fact_matches_statcast_batting_values",
    "plate_appearance_fact_uses_current_savant_revision",
}


def _selected_tests(
    selection: str, *, indirect_override: str | None = None
) -> set[str]:
    """List tests with a dummy profile and isolated parse/log artifacts.

    `dbt ls` resolves selection without querying Athena. Do not reuse the
    developer's target directory: a real build may be running concurrently.
    """

    with TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "profiles.yml").write_text(
            yaml.safe_dump(
                {
                    "zavant_analytics": {
                        "target": "selection_test",
                        "outputs": {
                            "selection_test": {
                                "type": "athena",
                                "database": "awsdatacatalog",
                                "schema": "selection_test",
                                "region_name": "us-east-1",
                                "s3_staging_dir": "s3://selection-test/results/",
                                "threads": 1,
                            }
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("DBT_")
        }
        environment["AWS_EC2_METADATA_DISABLED"] = "true"
        if indirect_override is not None:
            environment["DBT_INDIRECT_SELECTION"] = indirect_override
        result = subprocess.run(
            [
                str(Path(sys.executable).with_name("dbt")),
                "ls",
                "--quiet",
                "--no-send-anonymous-usage-stats",
                "--project-dir",
                str(_ROOT / "dbt"),
                "--profiles-dir",
                str(root),
                "--target-path",
                str(root / "target"),
                "--log-path",
                str(root / "logs"),
                "--resource-type",
                "test",
                "--output",
                "name",
                "--select",
                selection,
            ],
            cwd=_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if result.returncode:
            raise AssertionError(f"dbt ls failed:\n{result.stdout}\n{result.stderr}")
        return set(result.stdout.splitlines())


class DbtTestSelectionTests(unittest.TestCase):
    def test_savant_branch_defers_tests_requiring_downstream_facts(self) -> None:
        selected = _selected_tests(_SAVANT_MODELS)
        self.assertFalse(selected & _COMBINED_TESTS)
        self.assertIn("statcast_batting_events_use_current_date_revision", selected)
        self.assertIn("not_null_stg_statcast_batting_events_game_pk", selected)

    def test_combined_branch_keeps_tests_against_its_upstream_staging(self) -> None:
        selected = _selected_tests(_COMBINED_MODELS)
        self.assertTrue(_COMBINED_TESTS <= selected)
        self.assertIn("batted_ball_fact_uses_current_revision", selected)

    def test_dagster_can_override_default_when_checks_are_excluded(self) -> None:
        # Dagster sets this environment override for explicit check subsetting.
        self.assertEqual(
            _selected_tests(_SAVANT_MODELS, indirect_override="empty"), set()
        )
