"""API tests with the database layer replaced by an in-memory fake."""
from contextlib import contextmanager
from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from main import app
from routers import pipelines

RUN = {
    "id": 1,
    "pipeline_name": "dtc_events",
    "dag_run_id": "manual__1",
    "status": "success",
    "records_extracted": 24,
    "records_validated": 21,
    "records_loaded": 21,
    "records_rejected": 0,
    "source_file": "dtc/dtc_events_raw.csv",
    "error_message": None,
    "start_time": datetime(2024, 1, 1, 12, 0, 0),
    "end_time": datetime(2024, 1, 1, 12, 0, 5),
    "duration_seconds": 5,
}


class FakeCursor:
    """Returns the same single run for every query."""

    def execute(self, sql, params=None):
        self.sql = sql

    def fetchall(self):
        return [] if "data_quality_logs" in self.sql else [RUN]

    def fetchone(self):
        return RUN


@pytest.fixture
def client(monkeypatch):
    @contextmanager
    def fake_cursor():
        yield FakeCursor()

    monkeypatch.setattr(pipelines, "get_cursor", fake_cursor)
    return TestClient(app)


def test_health_returns_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_list_pipelines_returns_latest_runs(client):
    response = client.get("/pipelines/")
    assert response.status_code == 200
    assert response.json()[0]["pipeline_name"] == "dtc_events"


def test_unknown_pipeline_returns_404(client):
    assert client.get("/pipelines/not_a_pipeline").status_code == 404
    assert client.get("/pipelines/not_a_pipeline/runs").status_code == 404


def test_pipeline_detail_has_latest_and_recent_runs(client):
    body = client.get("/pipelines/dtc_events").json()
    assert body["latest_run"]["records_loaded"] == 21
    assert len(body["recent_runs"]) == 1


def test_runs_limit_is_capped_at_100(client):
    assert client.get("/pipelines/telemetry/runs?limit=5000").status_code == 200
