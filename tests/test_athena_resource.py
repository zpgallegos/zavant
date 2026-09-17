import unittest
from collections.abc import Mapping
from typing import Any

from zavant.orchestration.resources.athena import _execute_single_row_query


class _FakeAthenaQueryClient:
    def __init__(
        self,
        states: list[str],
        result: Mapping[str, Any] | None = None,
    ) -> None:
        self._states = iter(states)
        self._result = dict(result or {})
        self.start_requests: list[dict[str, Any]] = []
        self.status_requests: list[dict[str, Any]] = []
        self.result_requests: list[dict[str, Any]] = []

    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self.start_requests.append(kwargs)
        return {"QueryExecutionId": "query-42"}

    def get_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        self.status_requests.append(kwargs)
        state = next(self._states)
        return {
            "QueryExecution": {
                "Status": {
                    "State": state,
                    "StateChangeReason": "test failure",
                }
            }
        }

    def get_query_results(self, **kwargs: Any) -> dict[str, Any]:
        self.result_requests.append(kwargs)
        return self._result


class AthenaQueryResourceTests(unittest.TestCase):
    def test_waits_for_query_and_returns_one_named_row(self) -> None:
        client = _FakeAthenaQueryClient(
            states=["QUEUED", "RUNNING", "SUCCEEDED"],
            result={
                "ResultSet": {
                    "Rows": [
                        {
                            "Data": [
                                {"VarCharValue": "valid"},
                                {"VarCharValue": "optional"},
                            ]
                        },
                        {"Data": [{"VarCharValue": "1"}, {}]},
                    ]
                }
            },
        )
        sleeps: list[float] = []

        result = _execute_single_row_query(
            client=client,
            sql="SELECT 1",
            database="analytics",
            workgroup="primary",
            output_location="s3://bucket/results/",
            poll_interval_seconds=2,
            timeout_seconds=60,
            clock=lambda: 0,
            sleeper=sleeps.append,
        )

        self.assertEqual(result.query_execution_id, "query-42")
        self.assertEqual(result.row, {"valid": "1", "optional": None})
        self.assertEqual(sleeps, [2, 2])
        self.assertEqual(
            client.start_requests,
            [
                {
                    "QueryString": "SELECT 1",
                    "QueryExecutionContext": {"Database": "analytics"},
                    "ResultConfiguration": {"OutputLocation": "s3://bucket/results/"},
                    "WorkGroup": "primary",
                }
            ],
        )
        self.assertEqual(
            client.result_requests,
            [{"QueryExecutionId": "query-42", "MaxResults": 3}],
        )

    def test_rejects_more_than_one_data_row(self) -> None:
        client = _FakeAthenaQueryClient(
            states=["SUCCEEDED"],
            result={
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "count"}]},
                        {"Data": [{"VarCharValue": "1"}]},
                        {"Data": [{"VarCharValue": "2"}]},
                    ]
                }
            },
        )

        with self.assertRaisesRegex(RuntimeError, "exactly one data row"):
            _execute_single_row_query(
                client=client,
                sql="SELECT value FROM multiple_rows",
                database="analytics",
                workgroup="primary",
                output_location="s3://bucket/results/",
                poll_interval_seconds=2,
                timeout_seconds=60,
            )

    def test_raises_when_query_fails(self) -> None:
        client = _FakeAthenaQueryClient(states=["FAILED"])

        with self.assertRaisesRegex(RuntimeError, "FAILED: test failure"):
            _execute_single_row_query(
                client=client,
                sql="SELECT 1",
                database="analytics",
                workgroup="primary",
                output_location="s3://bucket/results/",
                poll_interval_seconds=2,
                timeout_seconds=60,
            )

    def test_raises_when_query_exceeds_timeout(self) -> None:
        client = _FakeAthenaQueryClient(states=["RUNNING"])
        clock_values = iter([0.0, 61.0])

        with self.assertRaisesRegex(TimeoutError, "within 60 seconds"):
            _execute_single_row_query(
                client=client,
                sql="SELECT 1",
                database="analytics",
                workgroup="primary",
                output_location="s3://bucket/results/",
                poll_interval_seconds=2,
                timeout_seconds=60,
                clock=lambda: next(clock_values),
            )


if __name__ == "__main__":
    unittest.main()
