"""Prepare dbt artifacts before starting a non-development code server."""

import subprocess

from zavant.orchestration.resources.dbt import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    DBT_TARGET,
    ZAVANT_DBT_RESOURCE,
)


def main() -> None:
    """Install dbt packages and parse the selected target without building data.

    Run this while the code server is stopped. The server imports the resulting
    manifest to define assets; it must use the same profile and target at runtime.
    A failed command aborts preparation instead of starting with stale artifacts.
    """

    for command in (["deps"], ["parse", "--no-partial-parse"]):
        subprocess.run(
            [
                ZAVANT_DBT_RESOURCE.dbt_executable,
                *command,
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROFILES_DIR),
                "--target",
                DBT_TARGET,
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
