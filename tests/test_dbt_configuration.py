import json
import re
import unittest
from pathlib import Path

from zavant.projection.contracts import PROJECTION_CONTRACT_VERSION


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_FILE = PROJECT_ROOT / "dbt" / "dbt_project.yml"
CONTRACT_VERSION_PATTERN = re.compile(
    r"^\s*current_projection_contract_version:\s*(?P<value>.+?)\s*$",
    re.MULTILINE,
)


class DbtConfigurationTest(unittest.TestCase):
    def test_projection_contract_matches_python(self) -> None:
        project_configuration = DBT_PROJECT_FILE.read_text(encoding="utf-8")
        matches = CONTRACT_VERSION_PATTERN.findall(project_configuration)

        self.assertEqual(len(matches), 1)
        self.assertEqual(json.loads(matches[0]), PROJECTION_CONTRACT_VERSION)


if __name__ == "__main__":
    unittest.main()
