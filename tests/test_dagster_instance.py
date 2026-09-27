import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import dagster as dg

# These storage records are test fixtures only. They let us verify sensor state
# without starting a daemon or evaluating any of the AWS-backed sensors.
from dagster._core.remote_origin import (
    RegisteredCodeLocationOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorStatus,
    InstigatorType,
    SensorInstigatorData,
    TickData,
    TickStatus,
)


_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_INSTANCE_TEMPLATE = _REPOSITORY_ROOT / "infrastructure" / "dagster" / "dagster.yaml"
_PROBE_ASSET_KEY = dg.AssetKey(["test_only", "instance_persistence"])


@dg.asset(key=_PROBE_ASSET_KEY)
def _persistence_probe() -> dg.MaterializeResult:
    """Produce a test-only event without touching any external dataset."""

    return dg.MaterializeResult(metadata={"purpose": "persistence test"})


def _sensor_state() -> InstigatorState:
    repository_origin = RemoteRepositoryOrigin(
        code_location_origin=RegisteredCodeLocationOrigin("persistence_test"),
        repository_name="test_repository",
    )
    return InstigatorState(
        origin=repository_origin.get_instigator_origin("test_sensor"),
        instigator_type=InstigatorType.SENSOR,
        status=InstigatorStatus.STOPPED,
        instigator_data=SensorInstigatorData(cursor="cursor-42"),
    )


def _persist_test_state() -> str:
    """Write all three storage categories from an isolated child process."""

    with dg.DagsterInstance.get() as instance:
        result = dg.materialize([_persistence_probe], instance=instance)
        assert result.success
        state = instance.add_instigator_state(_sensor_state())
        instance.create_tick(
            TickData(
                instigator_origin_id=state.instigator_origin_id,
                instigator_name=state.name,
                instigator_type=state.instigator_type,
                status=TickStatus.SKIPPED,
                timestamp=1_800_000_000.0,
                selector_id=state.selector_id,
                cursor="cursor-42",
                skip_reason="Persistence test only; no sensor evaluated.",
            )
        )
        return result.run_id


def _run_make(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "--no-print-directory", "DOTENV_FILE=/dev/null", *args],
        cwd=_REPOSITORY_ROOT,
        env={
            key: value
            for key, value in os.environ.items()
            if key not in {"DAGSTER_HOME", "MAKEFLAGS", "MFLAGS"}
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


class DagsterInstanceTests(unittest.TestCase):
    def test_dev_command_uses_the_persistent_home(self) -> None:
        result = _run_make("-n", "dagster-dev")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            f'DAGSTER_HOME="{_REPOSITORY_ROOT}/.local/dagster"', result.stdout
        )
        self.assertIn("infrastructure/dagster/dagster.yaml", result.stdout)
        self.assertIn("-m zavant.orchestration.definitions", result.stdout)

    def test_initialization_keeps_existing_configuration_and_history(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            instance_home = Path(temporary_directory) / "dagster home"
            result = _run_make("dagster-init", f"DAGSTER_HOME={instance_home}")
            self.assertEqual(result.returncode, 0, result.stderr)
            config_path = instance_home / "dagster.yaml"
            original_config = config_path.read_text(encoding="utf-8")
            self.assertEqual(
                original_config, _INSTANCE_TEMPLATE.read_text(encoding="utf-8")
            )

            custom_config = original_config + "\n# Local customization\n"
            config_path.write_text(custom_config, encoding="utf-8")
            marker_path = instance_home / "keep-history.txt"
            marker_path.write_text("existing history", encoding="utf-8")

            result = _run_make("dagster-init", f"DAGSTER_HOME={instance_home}")

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(config_path.read_text(encoding="utf-8"), custom_config)
            self.assertEqual(
                marker_path.read_text(encoding="utf-8"), "existing history"
            )
            command = _run_make("-n", "dagster-dev", f"DAGSTER_HOME={instance_home}")
            self.assertIn(f'DAGSTER_HOME="{instance_home}"', command.stdout)

    def test_local_instance_serializes_runs(self) -> None:
        with TemporaryDirectory() as directory:
            result = _run_make("dagster-init", f"DAGSTER_HOME={directory}")
            self.assertEqual(result.returncode, 0, result.stderr)
            with (
                patch.dict(os.environ, {"DAGSTER_HOME": directory}),
                dg.DagsterInstance.get() as instance,
            ):
                queue = instance.get_concurrency_config().run_queue_config
                self.assertIsNotNone(queue)
                assert queue is not None
                self.assertEqual(queue.max_concurrent_runs, 1)

    def test_initialization_rejects_empty_or_relative_home(self) -> None:
        for invalid_home in ("", "relative/dagster"):
            with self.subTest(instance_home=invalid_home):
                result = _run_make("dagster-init", f"DAGSTER_HOME={invalid_home}")

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("non-empty absolute path", result.stderr)

    def test_service_initialization_selects_queue_template_without_overwriting(
        self,
    ) -> None:
        service_template = _INSTANCE_TEMPLATE.with_name("dagster-service.yaml")
        with TemporaryDirectory() as directory:
            instance_home = Path(directory) / "services"
            result = _run_make("dagster-service-init", f"DAGSTER_HOME={instance_home}")
            self.assertEqual(result.returncode, 0, result.stderr)
            config = instance_home / "dagster.yaml"
            self.assertEqual(config.read_text(), service_template.read_text())
            result = _run_make("dagster-init", f"DAGSTER_HOME={instance_home}")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(config.read_text(), service_template.read_text())

            # Opting into the new command must not replace a customized home.
            config.write_text("# customized existing configuration\n")
            result = _run_make("dagster-service-init", f"DAGSTER_HOME={instance_home}")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                config.read_text(), "# customized existing configuration\n"
            )

    def test_instance_state_is_gitignored(self) -> None:
        paths = [
            ".local/dagster/history/runs.db",
            ".tmp_dagster_home_example/history/runs.db",
        ]
        result = subprocess.run(
            ["git", "check-ignore", *paths],
            cwd=_REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), paths)

    def test_run_events_and_sensor_state_survive_process_exit(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            instance_home = Path(temporary_directory) / "dagster"
            initialized = _run_make("dagster-init", f"DAGSTER_HOME={instance_home}")
            self.assertEqual(initialized.returncode, 0, initialized.stderr)
            writer = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import json; from tests.test_dagster_instance import "
                    "_persist_test_state; print(json.dumps(_persist_test_state()))",
                ],
                cwd=_REPOSITORY_ROOT,
                env={
                    **os.environ,
                    "DAGSTER_HOME": str(instance_home),
                    "PYTHONPATH": str(_REPOSITORY_ROOT / "src"),
                },
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            self.assertEqual(writer.returncode, 0, writer.stderr)
            run_id = json.loads(writer.stdout.strip().splitlines()[-1])

            # Repeat the initialization performed on each dev-server startup,
            # then reopen the same databases after the writer process exits.
            initialized = _run_make("dagster-init", f"DAGSTER_HOME={instance_home}")
            self.assertEqual(initialized.returncode, 0, initialized.stderr)
            with (
                patch.dict(os.environ, {"DAGSTER_HOME": str(instance_home)}),
                dg.DagsterInstance.get() as instance,
            ):
                self.assertTrue(instance.is_persistent)
                run = instance.get_run_by_id(run_id)
                self.assertIsNotNone(run)
                assert run is not None
                self.assertEqual(run.status, dg.DagsterRunStatus.SUCCESS)
                event = instance.get_latest_materialization_event(_PROBE_ASSET_KEY)
                self.assertIsNotNone(event)
                assert event is not None
                self.assertEqual(event.run_id, run_id)

                expected_state = _sensor_state()
                state = instance.get_instigator_state(
                    expected_state.instigator_origin_id, expected_state.selector_id
                )
                self.assertEqual(state, expected_state)
                ticks = instance.get_ticks(
                    expected_state.instigator_origin_id, expected_state.selector_id
                )
                self.assertEqual(len(ticks), 1)
                self.assertEqual(ticks[0].cursor, "cursor-42")
                self.assertEqual(ticks[0].status, TickStatus.SKIPPED)


if __name__ == "__main__":
    unittest.main()
