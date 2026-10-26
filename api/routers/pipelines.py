"""
Pipeline status endpoints.

All queries hit the fleet_dw database directly. The v_latest_pipeline_runs
view makes the /pipelines endpoint fast, no aggregation at query time.
"""
from fastapi import APIRouter, HTTPException
from models import DataQualityCheck, PipelineDetail, PipelineRunSummary

from database import get_cursor

router = APIRouter()

# Known pipeline names: used for validation so we return 404 instead of
# an empty result when someone typos the pipeline name
KNOWN_PIPELINES = {"telemetry", "dtc_events", "fleet_vehicles"}


@router.get("/", response_model=list[PipelineRunSummary])
def list_pipelines():
    """Return the latest run for each pipeline."""
    with get_cursor() as cur:
        cur.execute("SELECT * FROM v_latest_pipeline_runs ORDER BY pipeline_name")
        rows = cur.fetchall()
    return [PipelineRunSummary(**dict(row)) for row in rows]


@router.get("/{pipeline_name}", response_model=PipelineDetail)
def get_pipeline(pipeline_name: str):
    """
    Return the latest run + last 10 runs + data quality checks for a pipeline.

    Example: GET /pipelines/telemetry
    """
    if pipeline_name not in KNOWN_PIPELINES:
        raise HTTPException(
            status_code=404,
            detail=f"Pipeline '{pipeline_name}' not found. "
                   f"Known pipelines: {sorted(KNOWN_PIPELINES)}",
        )

    with get_cursor() as cur:
        # Latest run from the view (fast)
        cur.execute(
            "SELECT * FROM v_latest_pipeline_runs WHERE pipeline_name = %s",
            (pipeline_name,),
        )
        latest_row = cur.fetchone()

        # Last 10 runs (for run history)
        cur.execute(
            """
            SELECT
                id, pipeline_name, dag_run_id, status,
                records_extracted, records_validated, records_loaded, records_rejected,
                source_file, error_message, start_time, end_time,
                EXTRACT(EPOCH FROM (COALESCE(end_time, CURRENT_TIMESTAMP) - start_time))::INTEGER
                    AS duration_seconds
            FROM pipeline_runs
            WHERE pipeline_name = %s
            ORDER BY start_time DESC
            LIMIT 10
            """,
            (pipeline_name,),
        )
        run_rows = cur.fetchall()

        # Quality checks from the latest run
        quality_checks = []
        if latest_row:
            cur.execute(
                """
                SELECT id, check_name, status, records_checked, records_failed,
                       details, created_at
                FROM data_quality_logs
                WHERE pipeline_run_id = %s
                ORDER BY created_at
                """,
                (latest_row["id"],),
            )
            quality_checks = [DataQualityCheck(**dict(r)) for r in cur.fetchall()]

    return PipelineDetail(
        latest_run=PipelineRunSummary(**dict(latest_row)) if latest_row else None,
        recent_runs=[PipelineRunSummary(**dict(r)) for r in run_rows],
        quality_checks=quality_checks,
    )


@router.get("/{pipeline_name}/runs", response_model=list[PipelineRunSummary])
def list_pipeline_runs(pipeline_name: str, limit: int = 20):
    """Return up to `limit` recent runs for a pipeline (default 20, max 100)."""
    if pipeline_name not in KNOWN_PIPELINES:
        raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_name}' not found.")

    limit = min(limit, 100)  # cap so nobody accidentally dumps the whole table

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id, pipeline_name, dag_run_id, status,
                records_extracted, records_validated, records_loaded, records_rejected,
                source_file, error_message, start_time, end_time,
                EXTRACT(EPOCH FROM (COALESCE(end_time, CURRENT_TIMESTAMP) - start_time))::INTEGER
                    AS duration_seconds
            FROM pipeline_runs
            WHERE pipeline_name = %s
            ORDER BY start_time DESC
            LIMIT %s
            """,
            (pipeline_name, limit),
        )
        rows = cur.fetchall()

    return [PipelineRunSummary(**dict(r)) for r in rows]
