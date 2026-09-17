"""Optional outbound notifications; importing definitions never sends a message."""

import json
import os

import boto3
import dagster as dg


class SnsNotificationResource(dg.ConfigurableResource):
    """Publish run identifiers, not potentially sensitive exception text, to SNS.

    SNS delivery can be duplicated. Include the Dagster run ID so recipients
    can correlate messages. A failed publish raises, leaving a failed sensor
    tick visible in Dagster instead of silently dropping the notification.
    """

    topic_arn: str = ""
    region_name: str = "us-east-1"

    def notify_failure(self, job_name: str, run_id: str) -> None:
        if not self.topic_arn:
            raise RuntimeError(
                "Set ZAVANT_DAGSTER_ALERT_TOPIC_ARN before enabling alerts."
            )
        boto3.client("sns", region_name=self.region_name).publish(
            TopicArn=self.topic_arn,
            Subject="Zavant Dagster run failed",
            Message=json.dumps(
                {"job_name": job_name, "run_id": run_id, "status": "FAILURE"}
            ),
        )


DAGSTER_NOTIFICATION_RESOURCE = SnsNotificationResource(
    topic_arn=os.environ.get("ZAVANT_DAGSTER_ALERT_TOPIC_ARN", ""),
    region_name=os.environ.get("ZAVANT_AWS_REGION", "us-east-1"),
)
