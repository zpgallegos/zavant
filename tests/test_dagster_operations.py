import json
import os
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import dagster as dg
import yaml

# Exercise the installed queue policy against stored runs without a live daemon.
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.remote_origin import (
    RegisteredCodeLocationOrigin,
    RemoteRepositoryOrigin,
)
from dagster._daemon.run_coordinator.queued_run_coordinator_daemon import (
    QueuedRunCoordinatorDaemon,
)


from zavant.orchestration import prepare
from zavant.orchestration.resources.notifications import SnsNotificationResource
from zavant.orchestration.sensors.failures import notify_run_failure


_ROOT = Path(__file__).resolve().parents[1]
_SERVICE_CONFIG = _ROOT / "infrastructure/dagster/dagster-service.yaml"


class DagsterOperationsTests(unittest.TestCase):
    def test_prepare_installs_then_parses_without_building(self) -> None:
        with patch.object(prepare.subprocess, "run") as run:
            prepare.main()
        self.assertEqual(
            [call.args[0][1] for call in run.call_args_list], ["deps", "parse"]
        )
        for call in run.call_args_list:
            self.assertTrue(call.kwargs["check"])
            self.assertEqual(call.args[0][-2:], ["--target", prepare.DBT_TARGET])
            self.assertIn(str(prepare.DBT_PROFILES_DIR), call.args[0])
        with patch.object(
            prepare.subprocess,
            "run",
            side_effect=subprocess.CalledProcessError(1, "dbt"),
        ) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                prepare.main()
            run.assert_called_once()

    def test_dbt_target_can_be_selected_without_importing_assets(self) -> None:
        result = subprocess.run(
            [
                str(_ROOT / ".venv/bin/python"),
                "-c",
                "from zavant.orchestration.resources.dbt import ZAVANT_DBT_RESOURCE; "
                "assert ZAVANT_DBT_RESOURCE.target == 'prod'",
            ],
            cwd=_ROOT,
            env={**os.environ, "DBT_TARGET": "prod", "PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_service_instance_keeps_second_run_queued_until_first_finishes(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            instance_home = Path(directory)
            (instance_home / "dagster.yaml").write_text(_SERVICE_CONFIG.read_text())
            with (
                patch.dict(os.environ, {"DAGSTER_HOME": directory}),
                dg.DagsterInstance.get() as instance,
                QueuedRunCoordinatorDaemon(interval_seconds=1) as daemon,
            ):
                config = instance.get_concurrency_config()
                assert config.run_queue_config is not None
                self.assertEqual(config.run_queue_config.max_concurrent_runs, 1)
                running = instance.create_run_for_job(
                    dg.GraphDefinition(name="first").to_job(),
                    status=DagsterRunStatus.STARTED,
                )
                queued = instance.create_run_for_job(
                    dg.GraphDefinition(name="second").to_job(),
                    status=DagsterRunStatus.QUEUED,
                    remote_job_origin=RemoteRepositoryOrigin(
                        RegisteredCodeLocationOrigin("test"), "test_repository"
                    ).get_job_origin("second"),
                )
                self.assertEqual(
                    daemon._get_runs_to_dequeue(instance, config, None), []
                )
                instance.report_run_failed(running)
                self.assertEqual(
                    [
                        run.run_id
                        for run in daemon._get_runs_to_dequeue(instance, config, None)
                    ],
                    [queued.run_id],
                )

    def test_failure_sensor_publishes_identifiers_and_surfaces_publish_errors(
        self,
    ) -> None:
        resource = SnsNotificationResource(
            topic_arn="arn:aws:sns:us-east-1:123456789012:test"
        )
        with dg.DagsterInstance.ephemeral() as instance:
            run = instance.create_run_for_job(
                dg.GraphDefinition(name="failure_test").to_job()
            )
            failure = instance.report_run_failed(run)
            context = dg.build_run_status_sensor_context(
                sensor_name="notify_run_failure",
                dagster_event=failure,
                dagster_instance=instance,
                dagster_run=run,
            )
            with patch(
                "zavant.orchestration.resources.notifications.boto3.client"
            ) as client:
                notify_run_failure(context, notifications=resource)
                message = client.return_value.publish.call_args.kwargs
                self.assertEqual(json.loads(message["Message"])["run_id"], run.run_id)
                self.assertEqual(message["TopicArn"], resource.topic_arn)
                client.return_value.publish.side_effect = RuntimeError(
                    "SNS unavailable"
                )
                with self.assertRaisesRegex(RuntimeError, "SNS unavailable"):
                    notify_run_failure(context, notifications=resource)
        with patch(
            "zavant.orchestration.resources.notifications.boto3.client"
        ) as client:
            with self.assertRaisesRegex(
                RuntimeError, "Set ZAVANT_DAGSTER_ALERT_TOPIC_ARN"
            ):
                SnsNotificationResource().notify_failure("build_dbt", "test")
            client.assert_not_called()

    def test_local_service_workspace_uses_loopback(self) -> None:
        workspace = yaml.safe_load(
            (_ROOT / "infrastructure/dagster/workspace.yaml").read_text()
        )
        self.assertEqual(workspace["load_from"][0]["grpc_server"]["host"], "127.0.0.1")


if __name__ == "__main__":
    unittest.main()
