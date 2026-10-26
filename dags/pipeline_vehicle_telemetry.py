"""
DAG: pipeline_vehicle_telemetry

Extracts raw vehicle sensor readings from MinIO, validates them, transforms
with pandas, and loads to the fleet_dw PostgreSQL warehouse.

Schedule: every hour, telematics devices upload batches roughly on the hour.
Retry:    3 attempts with 5-minute delay. MinIO and Postgres are both inside
          Docker so most failures are transient (container restart, etc).
"""
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from etl.extract.minio_client import download_file, get_latest_object
from etl.load.postgres_loader import (
    complete_pipeline_run,
    create_pipeline_run,
    fail_pipeline_run,
    log_quality_check,
    upsert_telemetry,
)
from etl.monitoring.alerts import (
    log_data_quality_alert,
    on_pipeline_failure,
    on_pipeline_retry,
)
from etl.transform import telemetry as telemetry_transform
from etl.validate.validators import (
    check_required_columns_telemetry,
    check_sensor_ranges,
    check_speed_range,
    check_vin_format,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "telemetry"
RAW_PREFIX = "telemetry/"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_pipeline_failure,
    "on_retry_callback": on_pipeline_retry,
}


@dag(
    dag_id="pipeline_vehicle_telemetry",
    default_args=default_args,
    description="Vehicle telemetry sensor data: MinIO → validate → transform → PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["etl", "telemetry", "fleet"],
)
def telemetry_pipeline():

    @task
    def extract(**context) -> dict:
        """
        Find the latest telemetry file in MinIO and download it to /tmp.

        We pass file metadata (path, object key, run_id) as a dict to the next
        task via XCom. Airflow serialises XCom values as JSON, so keep this
        dict small: don't try to pass a DataFrame through XCom.
        """
        dag_run_id = context["run_id"]
        object_key = get_latest_object(prefix=RAW_PREFIX)

        if object_key is None:
            raise ValueError(
                f"No files found in MinIO under prefix '{RAW_PREFIX}'. "
                "Upload a telemetry CSV first (make trigger-all)."
            )

        run_id = create_pipeline_run(PIPELINE_NAME, dag_run_id, object_key)
        local_path = download_file(object_key)

        logger.info("Extracted %s → %s (pipeline_run_id=%d)", object_key, local_path, run_id)
        return {"local_path": local_path, "object_key": object_key, "run_id": run_id}

    @task
    def validate(extract_result: dict) -> dict:
        """
        Run data quality checks. Non-fatal failures are logged to data_quality_logs;
        only missing required columns causes the task to raise.
        """
        import pandas as pd

        local_path = extract_result["local_path"]
        run_id = extract_result["run_id"]

        # We read a preview here just for validation, transforms happen next task.
        # Using dtype=str so we check raw values before any casting.
        df = pd.read_csv(local_path, dtype=str)
        df = df.replace("None", pd.NA).replace("", pd.NA)

        checks = {
            "required_columns": check_required_columns_telemetry(df),
            "vin_format":        check_vin_format(df),
            "speed_range":       check_speed_range(df),
            "sensor_ranges":     check_sensor_ranges(df),
        }

        records_rejected = 0
        for check_name, result in checks.items():
            log_quality_check(run_id, check_name, result)
            if not result["passed"]:
                log_data_quality_alert(
                    PIPELINE_NAME, check_name,
                    result["details"], result["records_failed"]
                )
                records_rejected += result["records_failed"]
                # required_columns check failing means the file is broken, abort.
                if check_name == "required_columns":
                    fail_pipeline_run(run_id, result["details"])
                    raise ValueError(f"Required columns check failed: {result['details']}")

        logger.info(
            "Validation complete: %d rows checked, %d rejected",
            len(df), records_rejected,
        )
        return {**extract_result, "records_raw": len(df), "records_rejected": records_rejected}

    @task
    def transform(validate_result: dict) -> dict:
        """Apply pandas transformations to produce a clean DataFrame."""
        local_path = validate_result["local_path"]
        df = telemetry_transform.transform(local_path)
        # Save to a temp parquet so we don't re-read the CSV in the load task
        output_path = local_path.replace(".csv", "_transformed.parquet")
        df.to_parquet(output_path, index=False)
        return {**validate_result, "transformed_path": output_path, "records_transformed": len(df)}

    @task
    def load(transform_result: dict) -> dict:
        """Upsert transformed data to PostgreSQL."""
        import pandas as pd

        transformed_path = transform_result["transformed_path"]
        run_id = transform_result["run_id"]

        df = pd.read_parquet(transformed_path)
        n_loaded = upsert_telemetry(df, pipeline_run_id=run_id)
        return {**transform_result, "records_loaded": n_loaded}

    @task
    def finish(load_result: dict) -> None:
        """Mark the pipeline run as complete in the audit table."""
        complete_pipeline_run(
            run_id=load_result["run_id"],
            records_extracted=load_result["records_raw"],
            records_validated=load_result.get("records_transformed", 0),
            records_loaded=load_result["records_loaded"],
            records_rejected=load_result["records_rejected"],
        )
        logger.info(
            "Pipeline '%s' finished: extracted=%d loaded=%d rejected=%d",
            PIPELINE_NAME,
            load_result["records_raw"],
            load_result["records_loaded"],
            load_result["records_rejected"],
        )

    # Task dependency chain
    extracted  = extract()
    validated  = validate(extracted)
    transformed = transform(validated)
    loaded     = load(transformed)
    finish(loaded)


telemetry_pipeline()
