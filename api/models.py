"""Response models for the pipeline status API."""
from datetime import datetime

from pydantic import BaseModel


class PipelineRunSummary(BaseModel):
    id: int
    pipeline_name: str
    dag_run_id: str | None
    status: str
    records_extracted: int
    records_validated: int
    records_loaded: int
    records_rejected: int
    source_file: str | None
    error_message: str | None
    start_time: datetime
    end_time: datetime | None
    duration_seconds: int | None


class DataQualityCheck(BaseModel):
    id: int
    check_name: str
    status: str
    records_checked: int
    records_failed: int
    details: str | None
    created_at: datetime


class PipelineDetail(BaseModel):
    latest_run: PipelineRunSummary | None
    recent_runs: list[PipelineRunSummary]
    quality_checks: list[DataQualityCheck]
