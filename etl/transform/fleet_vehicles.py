"""
Fleet vehicle master data transformations.

This pipeline is simpler than telemetry/DTC but has its own gotcha: the status
field comes in with inconsistent capitalisation ('Active', 'ACTIVE', 'active').
Without this step a GROUP BY on status counts each spelling separately.
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Known-valid status values after normalisation
VALID_STATUSES = {"active", "inactive", "maintenance", "decommissioned", "sold"}


def load_raw_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, dtype=str)
    df = df.replace("None", pd.NA)
    df = df.replace("", pd.NA)
    logger.info("Loaded %d fleet vehicle rows from %s", len(df), filepath)
    return df


def normalise_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lowercase and strip the status column.

    Raw data has 'Active', 'ACTIVE' and 'active'. A GROUP BY on status would
    count them as three separate values, so they are normalised before load.
    """
    df = df.copy()
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    # Replace any unexpected value with NULL so we can investigate in data_quality_logs
    df.loc[~df["status"].isin(VALID_STATUSES), "status"] = pd.NA
    return df


def parse_assigned_date(df: pd.DataFrame) -> pd.DataFrame:
    if "assigned_date" in df.columns:
        df["assigned_date"] = pd.to_datetime(df["assigned_date"], errors="coerce").dt.date
    return df


def cast_vehicle_year(df: pd.DataFrame) -> pd.DataFrame:
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


def derive_is_active(df: pd.DataFrame) -> pd.DataFrame:
    """is_active = True only if status is 'active'. NULL status → False."""
    df["is_active"] = df["status"] == "active"
    return df


def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    output_cols = [
        "vin", "make", "model", "year",
        "license_plate", "plate_state",
        "fleet_id", "driver_id", "vehicle_type",
        "status", "assigned_date", "region",
        "is_active",
    ]
    available = [c for c in output_cols if c in df.columns]
    return df[available]


def transform(filepath: str) -> pd.DataFrame:
    df = load_raw_csv(filepath)
    df = parse_assigned_date(df)
    df = cast_vehicle_year(df)
    df = normalise_status(df)
    df = derive_is_active(df)
    df = select_output_columns(df)

    logger.info(
        "Fleet vehicles transform complete: %d vehicles (%d active)",
        len(df),
        int(df["is_active"].sum()),
    )
    return df
