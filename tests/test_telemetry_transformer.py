"""
Unit tests for the telemetry transform module.

These tests use small hand-crafted DataFrames instead of the full CSV so they
run fast and are easy to understand. The real CSV is tested through the DAG
integration tests (not written yet, see TODO below).

TODO: Add integration test that runs the full pipeline against a real MinIO/Postgres
instance in Docker. Right now we're only testing the transform logic in isolation.
"""
import pandas as pd

from etl.transform.telemetry import (
    cast_column_types,
    flag_sensor_outliers,
    remove_duplicate_records,
)


def make_basic_df(**overrides) -> pd.DataFrame:
    """Helper: a single-row telemetry DataFrame with sensible defaults."""
    base = {
        "record_id":       "TEL-0001",
        "vin":             "1FTFW1ET3DFB07826",
        "device_id":       "DEV-0001",
        "recorded_at":     "2024-01-08 07:14:02",
        "speed_mph":       "42.1",
        "rpm":             "2450",
        "fuel_level_pct":  "77.8",
        "oil_temp_f":      "201.3",
        "coolant_temp_f":  "190.2",
        "battery_voltage": "13.9",
        "latitude":        "41.8912",
        "longitude":       "-87.6150",
        "odometer_miles":  "34202.4",
        "ignition_status": "on",
    }
    base.update(overrides)
    return pd.DataFrame([base])


class TestCastColumnTypes:
    def test_converts_speed_to_float(self):
        df = cast_column_types(make_basic_df(speed_mph="55.3"))
        assert df["speed_mph"].dtype == float
        assert df["speed_mph"].iloc[0] == 55.3

    def test_converts_recorded_at_to_datetime(self):
        df = cast_column_types(make_basic_df())
        assert pd.api.types.is_datetime64_any_dtype(df["recorded_at"])

    def test_handles_none_string_already_replaced(self):
        # By the time cast_column_types runs, 'None' strings have been
        # replaced with pd.NA by load_raw_csv, so coerce should produce NaN
        df = make_basic_df(oil_temp_f=pd.NA)
        df = cast_column_types(df)
        assert pd.isna(df["oil_temp_f"].iloc[0])

    def test_non_numeric_string_becomes_nan(self):
        df = cast_column_types(make_basic_df(speed_mph="SENSOR_ERROR"))
        assert pd.isna(df["speed_mph"].iloc[0])


class TestRemoveDuplicateRecords:
    def test_removes_exact_duplicate_record_ids(self):
        df = pd.DataFrame([
            {**make_basic_df().iloc[0].to_dict()},
            {**make_basic_df().iloc[0].to_dict()},  # exact duplicate
        ])
        result = remove_duplicate_records(df)
        assert len(result) == 1

    def test_keeps_different_record_ids(self):
        df = pd.DataFrame([
            {**make_basic_df(record_id="TEL-0001").iloc[0].to_dict()},
            {**make_basic_df(record_id="TEL-0002").iloc[0].to_dict()},
        ])
        result = remove_duplicate_records(df)
        assert len(result) == 2


class TestFlagSensorOutliers:
    def test_normal_row_is_not_flagged(self):
        df = cast_column_types(make_basic_df(speed_mph="60.0"))
        result = flag_sensor_outliers(df)
        assert not result["is_outlier"].iloc[0]

    def test_speed_9999_is_flagged(self):
        # Sensor hardware fault case: speed of 9999
        df = cast_column_types(make_basic_df(speed_mph="9999.0"))
        result = flag_sensor_outliers(df)
        assert bool(result["is_outlier"].iloc[0])

    def test_negative_speed_is_flagged(self):
        df = cast_column_types(make_basic_df(speed_mph="-5.0"))
        result = flag_sensor_outliers(df)
        assert bool(result["is_outlier"].iloc[0])

    def test_null_speed_is_not_flagged(self):
        # Missing GPS reading: not an outlier, just a gap
        df = make_basic_df(speed_mph=pd.NA)
        df = cast_column_types(df)
        result = flag_sensor_outliers(df)
        assert not result["is_outlier"].iloc[0]

    def test_borderline_speed_not_flagged(self):
        # 250 mph is our threshold (test track vehicles can legitimately hit ~160)
        df = cast_column_types(make_basic_df(speed_mph="249.0"))
        result = flag_sensor_outliers(df)
        assert not result["is_outlier"].iloc[0]

    def test_battery_out_of_range_is_flagged(self):
        df = cast_column_types(make_basic_df(battery_voltage="3.0"))
        result = flag_sensor_outliers(df)
        assert bool(result["is_outlier"].iloc[0])
