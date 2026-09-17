"""Notify on failed dbt runs without automatically retrying warehouse writes."""

import dagster as dg

from zavant.orchestration.resources.notifications import SnsNotificationResource


@dg.run_failure_sensor(
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Send SNS notifications for failed runs in this code location.",
)
def notify_run_failure(
    context: dg.RunFailureSensorContext,
    notifications: SnsNotificationResource,
) -> None:
    """Notify for monitored dbt branches and manual asset runs."""

    notifications.notify_failure(
        context.dagster_run.job_name, context.dagster_run.run_id
    )
