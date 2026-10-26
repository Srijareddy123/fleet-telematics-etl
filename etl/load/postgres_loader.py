"""
PostgreSQL loading helpers.

We use raw psycopg2 here instead of SQLAlchemy ORM because the upsert logic
(INSERT ... ON CONFLICT DO UPDATE) is much clearer in plain SQL. ORMs make
simple upserts either verbose or magic.

Note: this opens a new connection per batch call. That is fine for the demo
batch sizes. A connection pool is listed under "What I'd do next" in the README.
"""
import logging
from collections.abc import Iterator
from contextlib import contextmanager

import pandas as pd
import psycopg2
import psycopg2.extras

from etl.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

logger = logging.getLogger(__name__)


@contextmanager
def get_connection() -> Iterator[psycopg2.extensions.connection]:
    """Context manager that opens a connection and auto-closes it."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        # Always close explicitly so long-running scheduler containers do not
        # leak file descriptors.
        conn.close()


def upsert_telemetry(df: pd.DataFrame, pipeline_run_id: int) -> int:
    """
    Load telemetry rows into fact_telemetry.

    Uses ON CONFLICT (record_id) DO UPDATE so re-running the same file is safe.
    The pipeline_run_id links each row back to the audit entry in pipeline_runs.
    """
    if df.empty:
        logger.warning("upsert_telemetry called with empty DataFrame, nothing to load")
        return 0

    sql = """
        INSERT INTO fact_telemetry (
            record_id, vin, device_id, recorded_at,
            speed_mph, rpm, fuel_level_pct,
            oil_temp_f, coolant_temp_f, battery_voltage,
            latitude, longitude, odometer_miles,
            ignition_status, is_outlier
        ) VALUES %s
        ON CONFLICT (record_id) DO UPDATE SET
            speed_mph       = EXCLUDED.speed_mph,
            rpm             = EXCLUDED.rpm,
            fuel_level_pct  = EXCLUDED.fuel_level_pct,
            oil_temp_f      = EXCLUDED.oil_temp_f,
            coolant_temp_f  = EXCLUDED.coolant_temp_f,
            battery_voltage = EXCLUDED.battery_voltage,
            is_outlier      = EXCLUDED.is_outlier,
            updated_at      = CURRENT_TIMESTAMP
    """

    rows = _df_to_tuples(df, [
        "record_id", "vin", "device_id", "recorded_at",
        "speed_mph", "rpm", "fuel_level_pct",
        "oil_temp_f", "coolant_temp_f", "battery_voltage",
        "latitude", "longitude", "odometer_miles",
        "ignition_status", "is_outlier",
    ])

    with get_connection() as conn:
        with conn.cursor() as cur:
            n_loaded = _bulk_execute(cur, sql, rows)

    # Also write to raw table for full audit trail
    _insert_raw_telemetry(df, pipeline_run_id)

    logger.info("Upserted %d telemetry rows", n_loaded)
    return n_loaded


def upsert_dtc_events(df: pd.DataFrame, pipeline_run_id: int) -> int:
    if df.empty:
        return 0

    sql = """
        INSERT INTO fact_dtc_events (
            event_id, vin, device_id, event_timestamp,
            dtc_code, dtc_description,
            severity_code, severity_label,
            is_active, cleared_at, mileage_at_event
        ) VALUES %s
        ON CONFLICT (event_id) DO UPDATE SET
            is_active       = EXCLUDED.is_active,
            cleared_at      = EXCLUDED.cleared_at,
            severity_code   = EXCLUDED.severity_code,
            severity_label  = EXCLUDED.severity_label,
            updated_at      = CURRENT_TIMESTAMP
    """

    rows = _df_to_tuples(df, [
        "event_id", "vin", "device_id", "event_timestamp",
        "dtc_code", "dtc_description",
        "severity_code", "severity_label",
        "is_active", "cleared_at", "mileage_at_event",
    ])

    with get_connection() as conn:
        with conn.cursor() as cur:
            n_loaded = _bulk_execute(cur, sql, rows)

    _insert_raw_dtc_events(df, pipeline_run_id)
    logger.info("Upserted %d DTC events", n_loaded)
    return n_loaded


def upsert_fleet_vehicles(df: pd.DataFrame, pipeline_run_id: int) -> int:
    if df.empty:
        return 0

    sql = """
        INSERT INTO dim_vehicles (
            vin, make, model, year,
            license_plate, plate_state,
            fleet_id, driver_id, vehicle_type,
            status, assigned_date, region, is_active
        ) VALUES %s
        ON CONFLICT (vin) DO UPDATE SET
            make          = EXCLUDED.make,
            model         = EXCLUDED.model,
            license_plate = EXCLUDED.license_plate,
            driver_id     = EXCLUDED.driver_id,
            status        = EXCLUDED.status,
            is_active     = EXCLUDED.is_active,
            updated_at    = CURRENT_TIMESTAMP
    """

    rows = _df_to_tuples(df, [
        "vin", "make", "model", "year",
        "license_plate", "plate_state",
        "fleet_id", "driver_id", "vehicle_type",
        "status", "assigned_date", "region", "is_active",
    ])

    with get_connection() as conn:
        with conn.cursor() as cur:
            n_loaded = _bulk_execute(cur, sql, rows)

    _insert_raw_fleet_vehicles(df, pipeline_run_id)
    logger.info("Upserted %d fleet vehicles", n_loaded)
    return n_loaded


# ── Pipeline run tracking ─────────────────────────────────────────────────────

def create_pipeline_run(pipeline_name: str, dag_run_id: str, source_file: str) -> int:
    """Insert a new pipeline_runs row with status='running'. Returns the run ID."""
    sql = """
        INSERT INTO pipeline_runs (pipeline_name, dag_run_id, status, source_file, start_time)
        VALUES (%s, %s, 'running', %s, CURRENT_TIMESTAMP)
        RETURNING id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (pipeline_name, dag_run_id, source_file))
            run_id = cur.fetchone()[0]
    logger.info("Created pipeline_run id=%d for %s", run_id, pipeline_name)
    return run_id


def complete_pipeline_run(run_id: int, records_extracted: int, records_validated: int,
                          records_loaded: int, records_rejected: int) -> None:
    sql = """
        UPDATE pipeline_runs SET
            status             = 'success',
            records_extracted  = %s,
            records_validated  = %s,
            records_loaded     = %s,
            records_rejected   = %s,
            end_time           = CURRENT_TIMESTAMP
        WHERE id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (records_extracted, records_validated, records_loaded,
                               records_rejected, run_id))


def fail_pipeline_run(run_id: int, error_message: str) -> None:
    sql = """
        UPDATE pipeline_runs SET
            status        = 'failed',
            error_message = %s,
            end_time      = CURRENT_TIMESTAMP
        WHERE id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (error_message, run_id))


def log_quality_check(pipeline_run_id: int, check_name: str, result: dict) -> None:
    sql = """
        INSERT INTO data_quality_logs
            (pipeline_run_id, check_name, status, records_checked, records_failed, details)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    status = "passed" if result["passed"] else "failed"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                pipeline_run_id,
                check_name,
                status,
                result.get("records_checked", 0),
                result.get("records_failed", 0),
                result.get("details", ""),
            ))


# ── Private helpers ────────────────────────────────────────────────────────────

PAGE_SIZE = 1000


def _bulk_execute(cur: psycopg2.extensions.cursor, sql: str, rows: list[tuple]) -> int:
    """
    Run a multi-row INSERT in pages and return the total affected row count.

    psycopg2's execute_values splits rows into pages and cur.rowcount only
    reflects the last page, so the count is summed here page by page.
    """
    total = 0
    for start in range(0, len(rows), PAGE_SIZE):
        psycopg2.extras.execute_values(
            cur, sql, rows[start:start + PAGE_SIZE], page_size=PAGE_SIZE
        )
        total += max(cur.rowcount, 0)
    return total

def _df_to_tuples(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    """
    Convert a DataFrame to a list of tuples for psycopg2.execute_values.

    psycopg2 only accepts native Python scalars, it cannot adapt numpy.int64,
    numpy.float64, or pandas NA types. We hit this with the `year` column in
    fleet_vehicles (cast to pandas Int64). Explicitly convert every value.
    """
    import numpy as np

    available = [c for c in columns if c in df.columns]
    result = []
    for row in df[available].itertuples(index=False):
        converted = []
        for v in row:
            if v is None or (not isinstance(v, str) and pd.isna(v)):
                converted.append(None)
            elif isinstance(v, np.integer):
                converted.append(int(v))
            elif isinstance(v, np.floating):
                converted.append(float(v))
            elif isinstance(v, np.bool_):
                converted.append(bool(v))
            else:
                converted.append(v)
        result.append(tuple(converted))
    return result


def _insert_raw_telemetry(df: pd.DataFrame, pipeline_run_id: int) -> None:
    sql = """
        INSERT INTO raw_telemetry (
            record_id, vin, device_id, recorded_at,
            speed_mph, rpm, fuel_level_pct,
            oil_temp_f, coolant_temp_f, battery_voltage,
            latitude, longitude, odometer_miles,
            ignition_status, pipeline_run_id
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """
    df_copy = df.copy()
    df_copy["pipeline_run_id"] = pipeline_run_id
    rows = _df_to_tuples(df_copy, [
        "record_id", "vin", "device_id", "recorded_at",
        "speed_mph", "rpm", "fuel_level_pct",
        "oil_temp_f", "coolant_temp_f", "battery_voltage",
        "latitude", "longitude", "odometer_miles",
        "ignition_status", "pipeline_run_id",
    ])
    with get_connection() as conn:
        with conn.cursor() as cur:
            _bulk_execute(cur, sql, rows)


def _insert_raw_dtc_events(df: pd.DataFrame, pipeline_run_id: int) -> None:
    sql = """
        INSERT INTO raw_dtc_events (
            event_id, vin, device_id, event_timestamp,
            dtc_code, dtc_description, severity,
            is_active, cleared_at, mileage_at_event, pipeline_run_id
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """
    df_copy = df.copy()
    df_copy["pipeline_run_id"] = pipeline_run_id
    # store the original severity string in raw
    if "severity" not in df_copy.columns:
        df_copy["severity"] = df_copy.get("severity_label")
    rows = _df_to_tuples(df_copy, [
        "event_id", "vin", "device_id", "event_timestamp",
        "dtc_code", "dtc_description", "severity",
        "is_active", "cleared_at", "mileage_at_event", "pipeline_run_id",
    ])
    with get_connection() as conn:
        with conn.cursor() as cur:
            _bulk_execute(cur, sql, rows)


def _insert_raw_fleet_vehicles(df: pd.DataFrame, pipeline_run_id: int) -> None:
    sql = """
        INSERT INTO raw_fleet_vehicles (
            vin, make, model, year, license_plate, plate_state,
            fleet_id, driver_id, vehicle_type, status,
            assigned_date, region, pipeline_run_id
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """
    df_copy = df.copy()
    df_copy["pipeline_run_id"] = pipeline_run_id
    rows = _df_to_tuples(df_copy, [
        "vin", "make", "model", "year", "license_plate", "plate_state",
        "fleet_id", "driver_id", "vehicle_type", "status",
        "assigned_date", "region", "pipeline_run_id",
    ])
    with get_connection() as conn:
        with conn.cursor() as cur:
            _bulk_execute(cur, sql, rows)
