"""Local dbt project and CLI resource shared by Dagster's dbt assets."""

import os
import sys
from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject


# Locate the repository from src/zavant/orchestration/resources/.
REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
DBT_PROJECT_DIR = REPOSITORY_ROOT / "dbt"
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", str(Path.home() / ".dbt")))
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")

ZAVANT_DBT_PROJECT = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    target=DBT_TARGET,
)
# Refresh the manifest only under Dagster's development-server workflow.
ZAVANT_DBT_PROJECT.prepare_if_dev()
ZAVANT_DBT_RESOURCE = DbtCliResource(
    project_dir=ZAVANT_DBT_PROJECT,
    # Use the dbt executable installed beside the active Dagster interpreter.
    dbt_executable=Path(sys.executable).with_name("dbt"),
)
