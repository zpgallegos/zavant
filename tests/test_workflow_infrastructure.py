import json
from pathlib import Path
import re
import textwrap
from typing import Any, Dict, cast
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ACQUISITION_TEMPLATE = (
    REPOSITORY_ROOT / "infrastructure" / "acquisition-stack.yaml"
)
WORKFLOW_TEMPLATE = (
    REPOSITORY_ROOT / "infrastructure" / "daily-workflow-stack.yaml"
)


class DailyWorkflowInfrastructureTests(unittest.TestCase):
    def test_acquisition_stack_has_no_workflow_or_schedule_resources(self) -> None:
        acquisition_template = ACQUISITION_TEMPLATE.read_text()

        self.assertNotIn("AWS::Scheduler::Schedule", acquisition_template)
        self.assertNotIn("DailyWorkflowArn", acquisition_template)
        self.assertNotIn("states:StartExecution", acquisition_template)

    def test_workflow_stack_owns_schedule_and_start_role(self) -> None:
        workflow_template = WORKFLOW_TEMPLATE.read_text()

        self.assertIn("Type: AWS::Scheduler::Schedule", workflow_template)
        self.assertIn("Action: states:StartExecution", workflow_template)
        self.assertIn("Arn: !Ref DailyWorkflowStateMachine", workflow_template)
        self.assertIn(
            "ScheduleExpression: !Ref DailyScheduleExpression",
            workflow_template,
        )
        self.assertIn("State: !Ref DailyScheduleState", workflow_template)

    def test_schedule_parameters_have_no_template_defaults(self) -> None:
        workflow_template = WORKFLOW_TEMPLATE.read_text()

        for parameter in (
            "DailyScheduleExpression",
            "DailyScheduleTimezone",
            "DailyScheduleState",
        ):
            self.assertNotIn("Default:", _parameter_block(workflow_template, parameter))

    def test_state_machine_runs_isolated_acquisitions_before_synchronous_glue(
        self,
    ) -> None:
        definition = _state_machine_definition(WORKFLOW_TEMPLATE.read_text())

        self.assertEqual(definition["StartAt"], "Prepare workflow input")
        preparation = definition["States"]["Prepare workflow input"]
        acquisitions = definition["States"]["Run source acquisitions"]
        projection = definition["States"]["Run analytical projection"]
        decision = definition["States"]["Did a source acquisition fail"]
        self.assertEqual(preparation["Next"], "Run source acquisitions")
        self.assertEqual(acquisitions["Type"], "Parallel")
        self.assertEqual(len(acquisitions["Branches"]), 2)
        branch_starts = {
            branch["StartAt"] for branch in acquisitions["Branches"]
        }
        self.assertEqual(
            branch_starts,
            {"Run Stats API acquisition", "Run Baseball Savant acquisition"},
        )
        for branch in acquisitions["Branches"]:
            task = branch["States"][branch["StartAt"]]
            self.assertEqual(task["Resource"], "arn:aws:states:::lambda:invoke")
            self.assertEqual(task["Parameters"]["Payload.$"], "$.request")
            self.assertEqual(task["Catch"][0]["ErrorEquals"], ["States.ALL"])
        self.assertEqual(acquisitions["Next"], "Run analytical projection")
        self.assertEqual(
            projection["Resource"], "arn:aws:states:::glue:startJobRun.sync"
        )
        self.assertEqual(projection["Next"], "Did a source acquisition fail")
        self.assertEqual(decision["Default"], "Workflow complete")
        self.assertEqual(len(decision["Choices"]), 2)
        self.assertTrue(
            all(
                choice["Next"] == "Report acquisition failure"
                for choice in decision["Choices"]
            )
        )

    def test_acquisition_stack_owns_separate_savant_lambda_and_role(self) -> None:
        template = ACQUISITION_TEMPLATE.read_text()

        self.assertIn(
            "Handler: zavant.ingestion.mlb_stats_api.lambda_handler.lambda_handler",
            template,
        )
        self.assertIn("BaseballSavantLambdaFunction:", template)
        self.assertIn("BaseballSavantLambdaExecutionRole:", template)
        self.assertIn(
            "Handler: zavant.ingestion.baseball_savant.lambda_handler.lambda_handler",
            template,
        )
        self.assertIn("/raw/baseball_savant/*", template)
        self.assertIn("/state/baseball_savant/*", template)

    def test_workflow_role_can_monitor_and_stop_the_glue_job(self) -> None:
        template = WORKFLOW_TEMPLATE.read_text()

        for action in (
            "glue:BatchStopJobRun",
            "glue:GetJobRun",
            "glue:GetJobRuns",
            "glue:StartJobRun",
        ):
            self.assertIn(action, template)


def _state_machine_definition(template: str) -> Dict[str, Any]:
    match = re.search(
        r"      DefinitionString: !Sub \|\n(?P<definition>.*?)"
        r"      LoggingConfiguration:",
        template,
        flags=re.DOTALL,
    )
    if match is None:
        raise AssertionError("workflow template has no inline definition")
    source = textwrap.dedent(match.group("definition"))
    source = source.replace("${AWS::Partition}", "aws")
    source = source.replace(
        "${AcquisitionLambdaArn}",
        "arn:aws:lambda:us-east-1:123456789012:function:acquisition",
    )
    source = source.replace(
        "${BaseballSavantLambdaArn}",
        "arn:aws:lambda:us-east-1:123456789012:function:baseball-savant",
    )
    source = source.replace("${AnalyticalProjectionJobName}", "projection-job")
    return cast(Dict[str, Any], json.loads(source))


def _parameter_block(template: str, parameter: str) -> str:
    start = template.index(f"  {parameter}:\n")
    match = re.search(r"\n(?:  [A-Z][A-Za-z]+:|Resources:)", template[start + 1 :])
    if match is None:
        raise AssertionError(f"parameter block has no boundary: {parameter}")
    return template[start : start + 1 + match.start()]


if __name__ == "__main__":
    unittest.main()
