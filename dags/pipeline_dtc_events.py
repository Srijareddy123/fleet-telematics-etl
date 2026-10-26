"""
DAG: pipeline_dtc_events

Processes Diagnostic Trouble Code (DTC) events uploaded by vehicle devices.
DTC events are critical: they indicate engine faults, emissions failures, and
safety-critical codes. Active P-codes are queryable in fact_dtc_events.

Key complexity: duplicate DTC delivery from old device firmware. See validators.
Schedule: every 4 hours, devices batch DTC events less frequently than telemetry.
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
    upsert_dtc_events,
)
from etl.monitoring.alerts import log_data_quality_alert, on_pipeline_failure, on_pipeline_retry
from etl.transform import dtc_events as dtc_transform
from etl.validate.validators import (
    check_dtc_code_format,
    check_duplicate_dtc_events,
    check_required_columns_dtc,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "dtc_events"
RAW_PREFIX = "dtc/"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_pipeline_failure,
    "on_retry_callback": on_pipeline_retry,
}


@dag(
    dag_id="pipeline_dtc_events",
    default_args=default_args,
    description="Vehicle DTC fault code events: MinIO → deduplicate → normalise → PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 */4 * * *",  # every 4 hours
    catchup=False,
    tags=["etl", "dtc", "fleet", "diagnostics"],
)
def dtc_pipeline():

    @task
    def extract(**context) -> dict:
        dag_run_id = context["run_id"]
        object_key = get_latest_object(prefix=RAW_PREFIX)

        if object_key is None:
            raise ValueError(f"No DTC files found in MinIO under '{RAW_PREFIX}'.")

        run_id = create_pipeline_run(PIPELINE_NAME, dag_run_id, object_key)
        local_path = download_file(object_key)

        return {"local_path": local_path, "object_key": object_key, "run_id": run_id}

    @task
    def validate(extract_result: dict) -> dict:
        import pandas as pd

        local_path = extract_result["local_path"]
        run_id = extract_result["run_id"]

        df = pd.read_csv(local_path, dtype=str)
        df = df.replace("None", pd.NA).replace("", pd.NA)

        checks = {
            "required_columns": check_required_columns_dtc(df),
            "dtc_code_format":  check_dtc_code_format(df),
            # We treat duplicates as a warning, not a hard failure, because
            # duplicate delivery is a known upstream issue we handle in transform.
            "duplicate_events": check_duplicate_dtc_events(df),
        }

        records_rejected = 0
        for check_name, result in checks.items():
            log_quality_check(run_id, check_name, result)
            if not result["passed"]:
                log_data_quality_alert(
                    PIPELINE_NAME, check_name,
                    result["details"], result["records_failed"]
                )
                # Duplicates are expected: don't count them as rejected rows
                if check_name != "duplicate_events":
                    records_rejected += result["records_failed"]

                if check_name == "required_columns":
                    fail_pipeline_run(run_id, result["details"])
                    raise ValueError(f"Required columns check failed: {result['details']}")

        return {**extract_result, "records_raw": len(df), "records_rejected": records_rejected}

    @task
    def transform(validate_result: dict) -> dict:
        local_path = validate_result["local_path"]
        df = dtc_transform.transform(local_path)
        output_path = local_path.replace(".csv", "_transformed.parquet")
        df.to_parquet(output_path, index=False)
        return {**validate_result, "transformed_path": output_path, "records_transformed": len(df)}

    @task
    def load(transform_result: dict) -> dict:
        import pandas as pd
        df = pd.read_parquet(transform_result["transformed_path"])
        n_loaded = upsert_dtc_events(df, pipeline_run_id=transform_result["run_id"])
        return {**transform_result, "records_loaded": n_loaded}

    @task
    def finish(load_result: dict) -> None:
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

    extracted   = extract()
    validated   = validate(extracted)
    transformed = transform(validated)
    loaded      = load(transformed)
    finish(loaded)


dtc_pipeline()
