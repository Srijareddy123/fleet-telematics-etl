"""
DAG: pipeline_fleet_vehicles

Syncs the vehicle master data file uploaded by the fleet operations team.
This is the simplest of the three pipelines, the data is managed manually
by ops (not by devices) so it's a lot cleaner.

Schedule: daily at 3am. The ops team updates the CSV at end-of-business;
3am gives them time to finish and gives us a quiet window to sync.
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
    upsert_fleet_vehicles,
)
from etl.monitoring.alerts import log_data_quality_alert, on_pipeline_failure, on_pipeline_retry
from etl.transform import fleet_vehicles as fleet_transform
from etl.validate.validators import (
    check_fleet_status_values,
    check_required_columns_fleet,
    check_vehicle_year,
    check_vin_format,
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "fleet_vehicles"
RAW_PREFIX = "vehicles/"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_pipeline_failure,
    "on_retry_callback": on_pipeline_retry,
}


@dag(
    dag_id="pipeline_fleet_vehicles",
    default_args=default_args,
    description="Fleet vehicle master data sync: MinIO → validate → normalise → PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",  # 3am daily
    catchup=False,
    tags=["etl", "fleet", "vehicles", "master-data"],
)
def fleet_vehicles_pipeline():

    @task
    def extract(**context) -> dict:
        dag_run_id = context["run_id"]
        object_key = get_latest_object(prefix=RAW_PREFIX)

        if object_key is None:
            raise ValueError(
                f"No fleet vehicle file found in MinIO under '{RAW_PREFIX}'. "
                "The ops team may not have uploaded today's file yet."
            )

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
            "required_columns": check_required_columns_fleet(df),
            "vin_format":        check_vin_format(df),
            "vehicle_year":      check_vehicle_year(df),
            # Status check is lenient, see check_fleet_status_values docstring
            "status_values":     check_fleet_status_values(df),
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

                if check_name == "required_columns":
                    fail_pipeline_run(run_id, result["details"])
                    raise ValueError(f"Required columns check failed: {result['details']}")

        return {**extract_result, "records_raw": len(df), "records_rejected": records_rejected}

    @task
    def transform(validate_result: dict) -> dict:
        local_path = validate_result["local_path"]
        df = fleet_transform.transform(local_path)
        output_path = local_path.replace(".csv", "_transformed.parquet")
        df.to_parquet(output_path, index=False)
        return {**validate_result, "transformed_path": output_path, "records_transformed": len(df)}

    @task
    def load(transform_result: dict) -> dict:
        import pandas as pd
        df = pd.read_parquet(transform_result["transformed_path"])
        n_loaded = upsert_fleet_vehicles(df, pipeline_run_id=transform_result["run_id"])
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
            "Pipeline '%s' finished: %d vehicles loaded",
            PIPELINE_NAME, load_result["records_loaded"],
        )

    extracted   = extract()
    validated   = validate(extracted)
    transformed = transform(validated)
    loaded      = load(transformed)
    finish(loaded)


fleet_vehicles_pipeline()
