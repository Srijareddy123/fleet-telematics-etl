"""
DTC (Diagnostic Trouble Code) event transformations.

The messiest of the three pipelines because DTC data comes from two different
device firmware generations with incompatible formats:

  - Old firmware (< v2.4):  severity = integer  (1=low, 2=medium, 3=high)
  - New firmware (>= v2.4): severity = string   ('low', 'medium', 'high')

We normalise to integer severity_code + string severity_label so downstream
queries don't need to care which firmware sent the event.
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Mapping from the various string values we've seen to a canonical integer.
# If you see a new value appearing in data_quality_logs, add it here.
_SEVERITY_STRING_MAP = {
    "low":    1,
    "medium": 2,
    "high":   3,
    "info":   1,   # some firmware uses 'info' for informational codes
    "warn":   2,
    "critical": 3,
}

_SEVERITY_LABEL_MAP = {1: "low", 2: "medium", 3: "high"}


def load_raw_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, dtype=str)
    df = df.replace("None", pd.NA)
    df = df.replace("", pd.NA)
    logger.info("Loaded %d DTC event rows from %s", len(df), filepath)
    return df


def parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("event_timestamp", "cleared_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def normalise_severity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert severity to a consistent (severity_code, severity_label) pair.

    This is the main reason DTC transforms are more complicated than telemetry.
    Two device generations, two formats, one table.
    """
    df = df.copy()

    def _to_code(val) -> int | None:
        if pd.isna(val):
            return None
        val_str = str(val).strip().lower()
        # Try integer first (old firmware)
        if val_str in ("1", "2", "3"):
            return int(val_str)
        # Fall back to string map (new firmware)
        return _SEVERITY_STRING_MAP.get(val_str)

    df["severity_code"] = df["severity"].apply(_to_code).astype("Int64")  # nullable int
    df["severity_label"] = df["severity_code"].map(_SEVERITY_LABEL_MAP)

    unmapped = df["severity_code"].isna() & df["severity"].notna()
    if unmapped.any():
        unknown_vals = df.loc[unmapped, "severity"].unique().tolist()
        logger.warning("Unmapped severity values (will be NULL in warehouse): %s", unknown_vals)

    return df


def normalise_is_active(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the is_active column from string ('true'/'false') to boolean.

    The raw CSV stores 'true' / 'false' as strings because the device platform
    exports all fields as text. We have to parse it manually.
    """
    df = df.copy()
    df["is_active"] = (
        df["is_active"]
        .astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False})
    )
    return df


def deduplicate_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate DTC events on (vin, dtc_code, event_timestamp).

    IMPORTANT: Keep the row with the lowest event_id so that if a device sends
    the same event twice with slightly different cleared_at values, we keep the
    first one (the one the device first reported). Keeping the last could
    accidentally mark a still-active fault as cleared.

    The sample data includes duplicate deliveries that mimic older firmware.
    """
    before = len(df)

    # Sort so keep='first' always keeps the lowest event_id
    df = df.sort_values("event_id").drop_duplicates(
        subset=["vin", "dtc_code", "event_timestamp"], keep="first"
    )

    dropped = before - len(df)
    if dropped > 0:
        logger.info("Deduplicated %d DTC events (firmware duplicate delivery)", dropped)

    return df


def cast_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "mileage_at_event" in df.columns:
        df["mileage_at_event"] = pd.to_numeric(df["mileage_at_event"], errors="coerce")
    return df


def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    output_cols = [
        "event_id", "vin", "device_id", "event_timestamp",
        "dtc_code", "dtc_description",
        "severity_code", "severity_label",
        "is_active", "cleared_at", "mileage_at_event",
    ]
    available = [c for c in output_cols if c in df.columns]
    return df[available]


def transform(filepath: str) -> pd.DataFrame:
    df = load_raw_csv(filepath)
    df = parse_timestamps(df)
    df = cast_numeric_columns(df)
    df = normalise_severity(df)
    df = normalise_is_active(df)
    df = deduplicate_events(df)
    df = select_output_columns(df)

    logger.info(
        "DTC transform complete: %d events (%d still active)",
        len(df),
        int(df["is_active"].eq(True).sum()),
    )
    return df
