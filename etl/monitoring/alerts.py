"""
Pipeline alerting.

Alerts are written to the Airflow task log as structured lines. The email and
Slack helpers below are left commented out; enabling them needs credentials in
environment variables.
"""
import logging

logger = logging.getLogger(__name__)


def on_pipeline_failure(context: dict) -> None:
    """
    Airflow on_failure_callback.

    Airflow passes a context dict with dag, task_instance, exception, etc.
    We log a structured message here so it shows up clearly in task logs.
    """
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")

    logger.error(
        "PIPELINE FAILURE | dag=%s | task=%s | execution_date=%s | error=%s",
        dag_id,
        task_id,
        execution_date,
        exception,
    )

    # To send to Slack:
    # _send_slack_alert(dag_id, task_id, execution_date, str(exception))

    # Or send an email:
    # _send_email_alert(dag_id, task_id, execution_date, str(exception))


def on_pipeline_retry(context: dict) -> None:
    """Log a warning when Airflow retries a task."""
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    try_number = context.get("task_instance").try_number
    exception = context.get("exception")

    logger.warning(
        "PIPELINE RETRY | dag=%s | task=%s | attempt=%s | error=%s",
        dag_id,
        task_id,
        try_number,
        exception,
    )


def log_data_quality_alert(pipeline_name: str, check_name: str, details: str,
                            records_failed: int) -> None:
    """
    Log a data quality issue. Called from the validate task when a check fails.

    Even when a check fails we usually let the pipeline continue (with failed rows
    rejected/flagged) rather than stopping entirely. This function records that
    something was wrong so the data team can investigate later.
    """
    logger.warning(
        "DATA QUALITY ALERT | pipeline=%s | check=%s | records_failed=%d | details=%s",
        pipeline_name,
        check_name,
        records_failed,
        details,
    )


# ── Private helpers (commented out until production credentials are available) ─

# def _send_slack_alert(dag_id, task_id, execution_date, error_msg):
#     import urllib.request, json
#     webhook_url = os.getenv("SLACK_WEBHOOK_URL")
#     if not webhook_url:
#         logger.warning("SLACK_WEBHOOK_URL not set: Slack alert skipped")
#         return
#     payload = {
#         "text": f":red_circle: *Pipeline failure*\n"
#                 f"DAG: `{dag_id}` | Task: `{task_id}`\n"
#                 f"Run: {execution_date}\n"
#                 f"Error: {error_msg[:500]}"
#     }
#     data = json.dumps(payload).encode("utf-8")
#     req = urllib.request.Request(webhook_url, data=data,
#                                  headers={"Content-Type": "application/json"})
#     urllib.request.urlopen(req, timeout=5)
