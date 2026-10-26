"""
Fleet ETL pipeline status API.

Exposes read-only endpoints so dashboards and analysts can check
pipeline health without needing direct database access.

Docs available at http://localhost:8000/docs when running locally.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import pipelines

app = FastAPI(
    title="Fleet ETL Pipeline Status API",
    description=(
        "Read-only API for monitoring ETL pipeline runs, data quality logs, "
        "and fleet data warehouse health."
    ),
    version="1.0.0",
)

# Allow browser dashboards to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # open for the local demo; restrict origins before exposing it
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])


@app.get("/health", tags=["health"])
def health_check():
    """Simple liveness probe. Returns 200 if the API is up."""
    return {"status": "ok", "version": "1.0.0"}


@app.get("/", include_in_schema=False)
def root():
    return {"message": "Fleet ETL API, see /docs for endpoints"}
