"""Measure validate, transform and (optionally) load throughput on generated telemetry.

Usage:
    python scripts/benchmark.py --rows 100000            # no database needed
    python scripts/benchmark.py --rows 100000 --load     # also upsert into Postgres

The sample telemetry file is repeated with fresh record_ids to reach the
requested row count. Results are printed and written to results/benchmark.json.
"""
from __future__ import annotations

import argparse
import json
import platform
import resource
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from etl.transform import telemetry as telemetry_transform  # noqa: E402
from etl.validate.validators import (  # noqa: E402
    check_required_columns_telemetry,
    check_sensor_ranges,
    check_speed_range,
    check_vin_format,
)

SAMPLE = ROOT / "sample_data" / "vehicle_telemetry_raw.csv"
OUT = ROOT / "results" / "benchmark.json"


def make_rows(n_rows: int) -> pd.DataFrame:
    """Repeat the sample file until it has n_rows rows, with unique record_ids."""
    base = pd.read_csv(SAMPLE, dtype=str)
    reps = -(-n_rows // len(base))
    df = pd.concat([base] * reps, ignore_index=True).head(n_rows).copy()
    df["record_id"] = [f"BENCH-{i:09d}" for i in range(len(df))]
    return df


def peak_rss_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024 * 1024) if platform.system() == "Darwin" else rss / 1024


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - start


def run_validate(path: Path) -> None:
    df = pd.read_csv(path, dtype=str).replace("None", pd.NA).replace("", pd.NA)
    check_required_columns_telemetry(df)
    check_vin_format(df)
    check_speed_range(df)
    check_sensor_ranges(df)


def run_load(df: pd.DataFrame) -> int:
    from etl.load.postgres_loader import (
        create_pipeline_run,
        get_connection,
        upsert_telemetry,
    )

    run_id = create_pipeline_run("benchmark", "benchmark", "generated")
    try:
        return upsert_telemetry(df, pipeline_run_id=run_id)
    finally:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM fact_telemetry WHERE record_id LIKE 'BENCH-%'")
            cur.execute("DELETE FROM raw_telemetry WHERE record_id LIKE 'BENCH-%'")
            cur.execute("DELETE FROM pipeline_runs WHERE id = %s", (run_id,))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--load", action="store_true", help="also upsert into Postgres")
    args = parser.parse_args()

    result: dict = {"rows": args.rows, "python": platform.python_version(),
                    "machine": platform.platform()}
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "telemetry.csv"
        make_rows(args.rows).to_csv(path, index=False)

        _, result["validate_seconds"] = timed(lambda: run_validate(path))
        df, result["transform_seconds"] = timed(lambda: telemetry_transform.transform(str(path)))
        result["rows_after_transform"] = len(df)
        if args.load:
            n, result["load_seconds"] = timed(lambda: run_load(df))
            result["rows_loaded"] = n

    for step in ("validate", "transform", "load"):
        if f"{step}_seconds" in result:
            result[f"{step}_rows_per_second"] = round(args.rows / result[f"{step}_seconds"])
            result[f"{step}_seconds"] = round(result[f"{step}_seconds"], 3)
    result["peak_rss_mb"] = round(peak_rss_mb(), 1)

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
