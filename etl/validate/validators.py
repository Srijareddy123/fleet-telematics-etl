"""
Data validation for all three pipelines.

Each validator returns a dict with:
  - passed: bool
  - records_checked: int
  - records_failed: int
  - details: str

Design decision: plain pandas checks here instead of a schema library such as
Great Expectations or Pandera. For batches in the tens of thousands of rows the
hand-rolled checks are fast (see scripts/benchmark.py) and easy to read.
"""
import logging

import pandas as pd

from etl.config import (
    BATTERY_VOLTAGE_MAX,
    BATTERY_VOLTAGE_MIN,
    FUEL_LEVEL_MAX_PCT,
    FUEL_LEVEL_MIN_PCT,
    OIL_TEMP_MAX_F,
    OIL_TEMP_MIN_F,
    RPM_MAX,
    SPEED_MAX_MPH,
)

logger = logging.getLogger(__name__)

# ── Type aliases ──────────────────────────────────────────────────────────────

CheckResult = dict  # {passed: bool, records_checked: int, records_failed: int, details: str}


# ── Telemetry validators ──────────────────────────────────────────────────────

def check_required_columns_telemetry(df: pd.DataFrame) -> CheckResult:
    """Every telemetry row must have at minimum a record_id, vin, and recorded_at."""
    required = ["record_id", "vin", "recorded_at"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        return {
            "passed": False,
            "records_checked": len(df),
            "records_failed": len(df),
            "details": f"Missing columns entirely: {missing_cols}",
        }

    null_mask = df[required].isnull().any(axis=1)
    n_failed = int(null_mask.sum())
    return {
        "passed": n_failed == 0,
        "records_checked": len(df),
        "records_failed": n_failed,
        "details": f"{n_failed} rows missing one of {required}",
    }


def check_vin_format(df: pd.DataFrame) -> CheckResult:
    """VINs must be exactly 17 alphanumeric characters, no I/O/Q."""
    # Standard VIN format per ISO 3779
    vin_pattern = r"^[A-HJ-NPR-Z0-9]{17}$"
    valid_mask = df["vin"].astype(str).str.match(vin_pattern, na=False)
    n_failed = int((~valid_mask).sum())
    return {
        "passed": n_failed == 0,
        "records_checked": len(df),
        "records_failed": n_failed,
        "details": f"{n_failed} rows with invalid VIN format",
    }


def check_speed_range(df: pd.DataFrame) -> CheckResult:
    """
    Flag rows where speed is outside plausible bounds.

    These rows get is_outlier=True instead of being dropped, so the raw
    reading stays queryable.
    """
    speed_col = pd.to_numeric(df["speed_mph"], errors="coerce")
    outlier_mask = (speed_col < 0) | (speed_col > SPEED_MAX_MPH)
    # NaN is handled separately (not an outlier, just missing)
    outlier_mask = outlier_mask & speed_col.notna()
    n_failed = int(outlier_mask.sum())
    return {
        "passed": n_failed == 0,
        "records_checked": int(speed_col.notna().sum()),
        "records_failed": n_failed,
        "details": f"{n_failed} rows with speed_mph outside [0, {SPEED_MAX_MPH}]",
    }


def check_sensor_ranges(df: pd.DataFrame) -> CheckResult:
    """
    Check that numeric sensor columns are within physically possible ranges.

    NULL values are allowed, sensors drop out (GPS in tunnels, cold-start oil
    temp, etc.). We only flag values that are present but nonsensical.
    """
    checks = {
        "rpm":             (0,                  RPM_MAX),
        "fuel_level_pct":  (FUEL_LEVEL_MIN_PCT, FUEL_LEVEL_MAX_PCT),
        "oil_temp_f":      (OIL_TEMP_MIN_F,     OIL_TEMP_MAX_F),
        "battery_voltage": (BATTERY_VOLTAGE_MIN, BATTERY_VOLTAGE_MAX),
    }

    total_failed = 0
    detail_parts = []

    for col, (lo, hi) in checks.items():
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        out_of_range = ((series < lo) | (series > hi)) & series.notna()
        n = int(out_of_range.sum())
        if n > 0:
            total_failed += n
            detail_parts.append(f"{col}: {n} rows outside [{lo}, {hi}]")

    return {
        "passed": total_failed == 0,
        "records_checked": len(df),
        "records_failed": total_failed,
        "details": "; ".join(detail_parts) if detail_parts else "all sensor ranges OK",
    }


# ── DTC event validators ──────────────────────────────────────────────────────

def check_required_columns_dtc(df: pd.DataFrame) -> CheckResult:
    required = ["event_id", "vin", "event_timestamp", "dtc_code"]
    null_mask = df[required].isnull().any(axis=1)
    n_failed = int(null_mask.sum())
    return {
        "passed": n_failed == 0,
        "records_checked": len(df),
        "records_failed": n_failed,
        "details": f"{n_failed} rows missing one of {required}",
    }


def check_dtc_code_format(df: pd.DataFrame) -> CheckResult:
    """
    DTC codes follow SAE J2012 format: one letter + 4 digits (e.g. P0420, C0035).
    This catches typos and malformed exports from the device management platform.
    """
    pattern = r"^[PBCU][0-9]{4}$"
    valid_mask = df["dtc_code"].astype(str).str.match(pattern, na=False)
    n_failed = int((~valid_mask & df["dtc_code"].notna()).sum())
    return {
        "passed": n_failed == 0,
        "records_checked": len(df),
        "records_failed": n_failed,
        "details": f"{n_failed} rows with malformed DTC code",
    }


def check_duplicate_dtc_events(df: pd.DataFrame) -> CheckResult:
    """
    Check for exact duplicates on (vin, dtc_code, event_timestamp).

    Some device firmware sends the same DTC twice within the same second.
    We detect duplicates here so the transform step can deduplicate before load.
    """
    key_cols = ["vin", "dtc_code", "event_timestamp"]
    available = [c for c in key_cols if c in df.columns]
    n_dupes = int(df.duplicated(subset=available).sum())
    return {
        "passed": n_dupes == 0,
        "records_checked": len(df),
        "records_failed": n_dupes,
        "details": f"{n_dupes} duplicate (vin, dtc_code, event_timestamp) combinations found",
    }


# ── Fleet vehicle validators ──────────────────────────────────────────────────

def check_required_columns_fleet(df: pd.DataFrame) -> CheckResult:
    required = ["vin", "make", "model", "year"]
    null_mask = df[required].isnull().any(axis=1)
    n_failed = int(null_mask.sum())
    return {
        "passed": n_failed == 0,
        "records_checked": len(df),
        "records_failed": n_failed,
        "details": f"{n_failed} rows missing one of {required}",
    }


def check_vehicle_year(df: pd.DataFrame) -> CheckResult:
    """Year must be between 1980 and current year + 1 (for pre-registered new models)."""
    import datetime
    current_year = datetime.date.today().year
    year_series = pd.to_numeric(df["year"], errors="coerce")
    bad_mask = (year_series < 1980) | (year_series > current_year + 1)
    bad_mask = bad_mask & year_series.notna()
    n_failed = int(bad_mask.sum())
    return {
        "passed": n_failed == 0,
        "records_checked": int(year_series.notna().sum()),
        "records_failed": n_failed,
        "details": f"{n_failed} rows with implausible vehicle year",
    }


def check_fleet_status_values(df: pd.DataFrame) -> CheckResult:
    """
    Status should be one of a known set after normalisation.

    NOTE: The raw CSV has 'Active', 'ACTIVE', 'active', 'maintenance' etc.
    This check runs on the RAW data before normalisation, it's intentionally
    lenient (case-insensitive). The transform step handles normalisation.
    """
    allowed = {"active", "inactive", "maintenance", "decommissioned", "sold"}
    normalised = df["status"].astype(str).str.strip().str.lower()
    bad_mask = ~normalised.isin(allowed) & df["status"].notna()
    n_failed = int(bad_mask.sum())
    return {
        "passed": n_failed == 0,
        "records_checked": int(df["status"].notna().sum()),
        "records_failed": n_failed,
        "details": f"{n_failed} rows with unrecognised status value: {df.loc[bad_mask, 'status'].unique().tolist()}",
    }
