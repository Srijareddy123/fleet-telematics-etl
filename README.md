# fleet-telematics-etl

A personal project: three Airflow pipelines that load messy vehicle telematics CSVs from MinIO (S3 compatible) into PostgreSQL, with data quality checks and a FastAPI status API. For data engineers who want to see a small orchestrated pipeline end to end.

Topics: `airflow`, `etl`, `data-engineering`, `fastapi`, `postgresql`, `minio`, `docker-compose`, `python`

![make demo output](docs/demo-output.png)

## Results

All data is small and generated for this repo. Per-row detail, caveats and benchmark setup: [docs/DETAILS.md](docs/DETAILS.md).

| What | Result | Command |
|---|---|---|
| Three sample pipelines (74, 24 and 10 rows) | 74, 21 (3 duplicate deliveries dropped) and 10 loaded, 1 row flagged as outlier, 4 to 6 s each | `make demo` |
| Validate and transform 100,000 generated telemetry rows | 0.27 s (about 370,000 rows/s) and 0.31 s (about 330,000 rows/s) | `make eval` |
| Upsert 100,000 rows into PostgreSQL | 7.1 s (about 14,000 rows/s) | `make bench-load` |
| Peak memory of the benchmark process | 310 MB | `make bench-load` |
| Unit and API tests | 52 pass | `make test` |

## How it works

```mermaid
flowchart LR
    CSV[Sample CSVs] --> MinIO[(MinIO)]
    MinIO --> AF[Airflow DAGs]
    AF --> PG[(PostgreSQL)]
    PG --> API[FastAPI /pipelines]
```

- Each DAG finds the newest file under its MinIO prefix, validates it, transforms it with pandas, and upserts it.
- Validation writes one row per check to `data_quality_logs`. A missing required column fails the run.
- Raw rows land in `raw_*` tables, cleaned rows in `fact_telemetry`, `fact_dtc_events` and `dim_vehicles`.
- Every run writes a `pipeline_runs` row that the API reads, so you can check status without database access.
- Tasks retry 3 times with a 5 minute delay and call a failure callback that logs a structured alert.

## Design choices

- **Flag outliers, do not drop them.** A bad sensor reading is still evidence, so it loads with `is_outlier = TRUE`.
- **Hand-written pandas checks, not Great Expectations.** They run in well under a second and the failure messages are easy to read.
- **Upserts keyed on a business id.** Re-running a file is safe for the curated tables.
- **A plain retry loop for MinIO downloads.** It is three lines and easy to step through.
- **MinIO and PostgreSQL stand in for S3 and Redshift.** Not deployed to AWS; deployable by configuration only.

## Run it

Needs Docker and `python3` on your PATH. The five containers used about 1.6 GB of memory in my run (`docker stats`). No API keys.

```bash
git clone https://github.com/Srijareddy123/fleet-telematics-etl.git
cd fleet-telematics-etl
make demo
```

This copies `.env.example` to `.env`, starts PostgreSQL, MinIO, Airflow (webserver and scheduler) and the API, uploads the sample files, triggers the three DAGs and prints the table above. First run builds images and takes a few minutes.

If a host port is taken, change the `*_HOST_PORT` values in `.env`.

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin123 |
| API docs | http://localhost:8000/docs | none |

```bash
make down    # stop
make clean   # stop and delete all data
```

## Tests

```bash
python3 -m venv .venv && . .venv/bin/activate   # Python 3.12 or 3.13
pip install -r requirements.txt
make test    # 52 tests: transforms, validators, loader helpers, API with a fake database
make lint    # ruff
make eval    # throughput benchmark, no Docker needed
```

Use Python 3.12 or 3.13 for the tests and install: Python 3.14 fails on the psycopg2-binary pin. CI runs lint, tests and the benchmark on Python 3.12 and 3.13. The Docker images use Python 3.11 because Airflow 2.9 ships with it.

## What I'd do next

- Process every new file under a prefix, not just the newest one, and add a backfill DAG.
- Use a connection pool in the loader. Today each batch opens its own connection.
- Count distinct rejected rows instead of summing failures per check.
- Wire the Slack and email alert helpers, which are commented out in `etl/monitoring/alerts.py`.
- Add an integration test that runs a DAG against real PostgreSQL and MinIO in CI.
