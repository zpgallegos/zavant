"""Opt-in process smoke test: real services, isolated state, no AWS execution."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import URLError
from urllib.request import Request, urlopen
from unittest.mock import patch

import dagster as dg


_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _stop_service(process: subprocess.Popen[bytes]) -> None:
    # Kill only process groups created by this test, including Dagster children.
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)


@unittest.skipUnless(
    os.environ.get("ZAVANT_TEST_DAGSTER_SERVICES") == "1",
    "Set ZAVANT_TEST_DAGSTER_SERVICES=1 to start isolated local services.",
)
class DagsterServiceTests(unittest.TestCase):
    def test_three_services_load_real_definitions_without_launching_runs(self) -> None:
        with TemporaryDirectory() as directory, ExitStack() as stack:
            home = Path(directory)
            (home / "dagster.yaml").write_text(
                (_ROOT / "infrastructure/dagster/dagster-service.yaml").read_text()
            )
            code_port, web_port = _free_port(), _free_port()
            while code_port == web_port:
                web_port = _free_port()
            workspace = home / "workspace.yaml"
            workspace.write_text(
                json.dumps(
                    {
                        "load_from": [
                            {
                                "grpc_server": {
                                    "host": "127.0.0.1",
                                    "port": code_port,
                                    "location_name": "zavant.orchestration.definitions",
                                }
                            }
                        ]
                    }
                )
            )
            environment = {
                **os.environ,
                "DAGSTER_HOME": directory,
                "PYTHONPATH": str(_ROOT / "src"),
                "AWS_EC2_METADATA_DISABLED": "true",
            }
            commands = {
                "code": [
                    "dagster",
                    "api",
                    "grpc",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    str(code_port),
                    "-m",
                    "zavant.orchestration.definitions",
                ],
                "webserver": [
                    "dagster-webserver",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    str(web_port),
                    "-w",
                    str(workspace),
                ],
                "daemon": ["dagster-daemon", "run", "-w", str(workspace)],
            }
            processes = []
            for name, command in commands.items():
                log = stack.enter_context((home / f"{name}.log").open("wb"))
                process = subprocess.Popen(
                    [str(Path(sys.executable).with_name(command[0])), *command[1:]],
                    cwd=_ROOT,
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                processes.append(process)
                stack.callback(_stop_service, process)

            request = Request(
                f"http://127.0.0.1:{web_port}/graphql",
                data=json.dumps(
                    {
                        "query": "{ repositoriesOrError { __typename ... on RepositoryConnection { nodes { pipelines { name } } } } }"
                    }
                ).encode(),
                headers={"Content-Type": "application/json"},
            )
            deadline = time.monotonic() + 45
            with patch.dict(os.environ, {"DAGSTER_HOME": directory}):
                while time.monotonic() < deadline:
                    if any(process.poll() is not None for process in processes):
                        break
                    try:
                        with urlopen(request, timeout=2) as response:
                            body = json.load(response)
                        repositories = body.get("data", {}).get(
                            "repositoriesOrError", {}
                        )
                        jobs = [
                            job["name"]
                            for repo in repositories.get("nodes", [])
                            for job in repo["pipelines"]
                        ]
                        with dg.DagsterInstance.get() as instance:
                            heartbeats = instance.get_daemon_heartbeats()
                            if "build_dbt" in jobs and {
                                "SENSOR",
                                "SCHEDULER",
                                "QUEUED_RUN_COORDINATOR",
                            } <= set(heartbeats):
                                self.assertEqual(instance.get_runs(), [])
                                self.assertTrue(
                                    all(
                                        not heartbeat.errors
                                        for heartbeat in heartbeats.values()
                                    )
                                )
                                return
                    except (URLError, TimeoutError):
                        pass
                    time.sleep(0.5)
            logs = "\n".join(path.read_text() for path in home.glob("*.log"))
            self.fail(f"Services did not become healthy:\n{logs}")


if __name__ == "__main__":
    unittest.main()
