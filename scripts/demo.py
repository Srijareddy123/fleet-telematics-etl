"""Trigger the three DAGs on the running stack and print what landed in the warehouse.

Run it through `make demo` after `docker compose up -d`. It needs the Docker
Compose stack for the Airflow CLI and the status API for the results.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def env_value(name: str, default: str) -> str:
    """Read a value from the environment, then from .env, then use the default."""
    if name in os.environ:
        return os.environ[name]
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            key, _, value = line.partition("=")
            if key.strip() == name:
                return value.strip()
    return default


API = f"http://localhost:{env_value('API_HOST_PORT', '8000')}"
DAGS = {
    "pipeline_vehicle_telemetry": "telemetry",
    "pipeline_dtc_events": "dtc_events",
    "pipeline_fleet_vehicles": "fleet_vehicles",
}
TIMEOUT_SECONDS = 420


def airflow(*args: str) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "exec", "-T", "airflow-webserver", "airflow", *args]
    return subprocess.run(cmd, capture_output=True, text=True)


def wait_for_api() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{API}/health", timeout=3) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, OSError):
            time.sleep(3)
    sys.exit("The status API did not come up. Check `docker compose ps`.")


def trigger(dag_id: str) -> None:
    deadline = time.time() + 240
    while time.time() < deadline:
        airflow("dags", "unpause", dag_id)
        if airflow("dags", "trigger", dag_id).returncode == 0:
            return
        time.sleep(5)  # the scheduler has not parsed the DAG file yet
    sys.exit(f"Could not trigger {dag_id}.")


def latest_runs() -> list[dict]:
    with urllib.request.urlopen(f"{API}/pipelines/", timeout=10) as resp:
        return json.load(resp)


def main() -> None:
    wait_for_api()
    # Remember the newest run id per pipeline so old runs are not mistaken for new ones.
    baseline = {r["pipeline_name"]: r["id"] for r in latest_runs()}
    triggered_at = time.time()
    for dag_id in DAGS:
        trigger(dag_id)
        print(f"triggered {dag_id}")

    # A run counts only if its id is newer than the baseline.
    while time.time() - triggered_at < TIMEOUT_SECONDS:
        runs = {r["pipeline_name"]: r for r in latest_runs()}
        done = [
            n for n in DAGS.values()
            if runs.get(n, {}).get("id", 0) > baseline.get(n, 0)
            and runs[n]["status"] in ("success", "failed")
        ]
        if len(done) == len(DAGS):
            break
        time.sleep(5)
    else:
        sys.exit("Timed out waiting for the pipelines to finish.")

    print(f"\n{'pipeline':<16}{'status':<9}{'extracted':>10}{'loaded':>8}{'rejected':>9}{'seconds':>9}")
    failed = False
    for name in DAGS.values():
        r = runs[name]
        failed |= r["status"] != "success"
        print(f"{name:<16}{r['status']:<9}{r['records_extracted']:>10}"
              f"{r['records_loaded']:>8}{r['records_rejected']:>9}{r['duration_seconds']:>9}")
    print(f"\nFastAPI docs: {API}/docs")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
