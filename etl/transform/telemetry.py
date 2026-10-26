"""
Telemetry data transformations.

Input:  raw CSV as downloaded from MinIO (vehicle_telemetry_raw.csv)
Output: cleaned DataFrame ready to upsert into fact_telemetry

Key issues this transform handles:
  1. 'None' string literals from older device firmware
  2. Speed/sensor outliers flagged instead of dropped
  3. Duplicate records from devices that send the same reading twice
  4. Mixed timestamp formats across different device firmware versions
"""
import logging

import pandas as pd

from etl.config import (
    BATTERY_VOLTAGE_MAX,
    BATTERY_VOLTAGE_MIN,
    FUEL_LEVEL_MAX_PCT,
    FUEL_LEVEL_MIN_PCT,
    RPM_MAX,
    SPEED_MAX_MPH,
)

logger = logging.getLogger(__name__)


def load_raw_csv(filepath: str) -> pd.DataFrame:
    """
    Read the raw CSV and immediately fix the 'None'-as-string problem.

    Some older telematics devices send the literal string
    'None' instead of an empty field. pandas treats it as a valid string,
    which then breaks all numeric operations downstream. We convert all
    'None' and empty strings to proper NaN right at ingestion.
    """
    df = pd.read_csv(filepath, dtype=str)  # read everything as string first

    # Replace both 'None' strings and empty strings with NaN
    df = df.replace("None", pd.NA)
    df = df.replace("", pd.NA)

    logger.info("Loaded %d rows from %s", len(df), filepath)
    return df


def cast_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast columns to their correct types after the 'None' cleanup.

    Doing this step separately from load_raw_csv makes it easier to add a new
    column without hunting through a giant read_csv call with dtype= arguments.
    """
    numeric_cols = [
        "speed_mph", "rpm", "fuel_level_pct",
        "oil_temp_f", "coolant_temp_f", "battery_voltage",
        "latitude", "longitude", "odometer_miles",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "recorded_at" in df.columns:
        df["recorded_at"] = pd.to_datetime(df["recorded_at"], errors="coerce")

    return df


def remove_duplicate_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows that are exact duplicates on (record_id).

    This is different from the DTC duplicate check, here we trust record_id
    as a unique key per reading. If two rows share the same record_id the
    device sent the same message twice and we keep only the first occurrence.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["record_id"], keep="first")
    dropped = before - len(df)
    if dropped > 0:
        logger.warning("Dropped %d duplicate record_ids", dropped)
    return df


def flag_sensor_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add an is_outlier boolean column instead of deleting bad rows.

    Keeping the row with is_outlier=True lets analysts query it; dropping it
    would make it invisible in the warehouse entirely.
    """
    df = df.copy()  # don't mutate the caller's DataFrame

    outlier_mask = pd.Series(False, index=df.index)

    if "speed_mph" in df.columns:
        outlier_mask |= (df["speed_mph"] > SPEED_MAX_MPH) & df["speed_mph"].notna()
        outlier_mask |= (df["speed_mph"] < 0) & df["speed_mph"].notna()

    if "rpm" in df.columns:
        outlier_mask |= (df["rpm"] > RPM_MAX) & df["rpm"].notna()

    if "fuel_level_pct" in df.columns:
        outlier_mask |= (
            (df["fuel_level_pct"] < FUEL_LEVEL_MIN_PCT) |
            (df["fuel_level_pct"] > FUEL_LEVEL_MAX_PCT)
        ) & df["fuel_level_pct"].notna()

    if "battery_voltage" in df.columns:
        outlier_mask |= (
            (df["battery_voltage"] < BATTERY_VOLTAGE_MIN) |
            (df["battery_voltage"] > BATTERY_VOLTAGE_MAX)
        ) & df["battery_voltage"].notna()

    df["is_outlier"] = outlier_mask
    n_outliers = int(outlier_mask.sum())
    if n_outliers > 0:
        logger.warning("Flagged %d rows as sensor outliers", n_outliers)

    return df


def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Select and order the columns that map to fact_telemetry."""
    output_cols = [
        "record_id", "vin", "device_id", "recorded_at",
        "speed_mph", "rpm", "fuel_level_pct",
        "oil_temp_f", "coolant_temp_f", "battery_voltage",
        "latitude", "longitude", "odometer_miles",
        "ignition_status", "is_outlier",
    ]
    # Only include columns that exist in the DataFrame
    available = [c for c in output_cols if c in df.columns]
    return df[available]


def transform(filepath: str) -> pd.DataFrame:
    """
    Full transformation pipeline for telemetry data.

    Each step is a separate function so unit tests can exercise them individually.
    """
    df = load_raw_csv(filepath)
    df = cast_column_types(df)
    df = remove_duplicate_records(df)
    df = flag_sensor_outliers(df)
    df = select_output_columns(df)

    logger.info(
        "Telemetry transform complete: %d rows, %d outliers flagged",
        len(df),
        int(df["is_outlier"].sum()),
    )
    return df
