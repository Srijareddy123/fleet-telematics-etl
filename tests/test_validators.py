"""Tests for the data quality validators."""
import pandas as pd

from etl.validate.validators import (
    check_dtc_code_format,
    check_duplicate_dtc_events,
    check_required_columns_telemetry,
    check_sensor_ranges,
    check_speed_range,
    check_vin_format,
)


def make_telemetry_df(**overrides) -> pd.DataFrame:
    base = {
        "record_id":   "TEL-0001",
        "vin":         "1FTFW1ET3DFB07826",
        "recorded_at": "2024-01-08 07:14:02",
        "speed_mph":   42.1,
        "rpm":         2450.0,
        "fuel_level_pct": 77.8,
        "battery_voltage": 13.9,
    }
    base.update(overrides)
    return pd.DataFrame([base])


class TestRequiredColumnsTelemetry:
    def test_passes_when_all_required_present(self):
        result = check_required_columns_telemetry(make_telemetry_df())
        assert result["passed"] is True

    def test_fails_when_record_id_is_null(self):
        result = check_required_columns_telemetry(make_telemetry_df(record_id=None))
        assert result["passed"] is False
        assert result["records_failed"] == 1

    def test_fails_when_vin_missing(self):
        df = make_telemetry_df()
        df = df.drop(columns=["vin"])
        result = check_required_columns_telemetry(df)
        assert result["passed"] is False


class TestVinFormat:
    def test_valid_vin_passes(self):
        result = check_vin_format(make_telemetry_df(vin="1FTFW1ET3DFB07826"))
        assert result["passed"] is True

    def test_vin_too_short_fails(self):
        result = check_vin_format(make_telemetry_df(vin="1FTFW1ET3"))
        assert result["passed"] is False

    def test_vin_with_letter_i_fails(self):
        # VIN standard (ISO 3779) forbids I, O, Q
        result = check_vin_format(make_telemetry_df(vin="IFTFW1ET3DFB07826"))
        assert result["passed"] is False

    def test_vin_with_lowercase_fails(self):
        result = check_vin_format(make_telemetry_df(vin="1ftfw1et3dfb07826"))
        assert result["passed"] is False


class TestSpeedRange:
    def test_normal_speed_passes(self):
        result = check_speed_range(make_telemetry_df(speed_mph=65.0))
        assert result["passed"] is True

    def test_zero_speed_passes(self):
        # Zero = vehicle stopped at a light, perfectly valid
        result = check_speed_range(make_telemetry_df(speed_mph=0.0))
        assert result["passed"] is True

    def test_9999_speed_fails(self):
        # The DEV-0023 sensor fault value
        result = check_speed_range(make_telemetry_df(speed_mph=9999.0))
        assert result["passed"] is False

    def test_null_speed_passes(self):
        # GPS dropout: not an outlier
        result = check_speed_range(make_telemetry_df(speed_mph=None))
        assert result["passed"] is True


class TestSensorRanges:
    def test_all_in_range_passes(self):
        result = check_sensor_ranges(make_telemetry_df())
        assert result["passed"] is True

    def test_fuel_over_100_fails(self):
        result = check_sensor_ranges(make_telemetry_df(fuel_level_pct=105.0))
        assert result["passed"] is False

    def test_battery_below_9v_fails(self):
        result = check_sensor_ranges(make_telemetry_df(battery_voltage=3.5))
        assert result["passed"] is False

    def test_null_sensor_value_passes(self):
        # Cold-start oil temp sensor, NULL is acceptable
        result = check_sensor_ranges(make_telemetry_df(battery_voltage=None))
        assert result["passed"] is True


class TestDtcCodeFormat:
    def test_valid_p_code(self):
        df = pd.DataFrame([{"dtc_code": "P0420"}])
        result = check_dtc_code_format(df)
        assert result["passed"] is True

    def test_valid_c_code(self):
        df = pd.DataFrame([{"dtc_code": "C0035"}])
        result = check_dtc_code_format(df)
        assert result["passed"] is True

    def test_malformed_code_fails(self):
        df = pd.DataFrame([{"dtc_code": "XXXX123"}])
        result = check_dtc_code_format(df)
        assert result["passed"] is False

    def test_lowercase_fails(self):
        df = pd.DataFrame([{"dtc_code": "p0420"}])
        result = check_dtc_code_format(df)
        assert result["passed"] is False


class TestDuplicateDtcEvents:
    def test_no_duplicates_passes(self):
        df = pd.DataFrame([
            {"vin": "1FTFW1ET3DFB07826", "dtc_code": "P0420", "event_timestamp": "2024-01-09 09:06:30"},
            {"vin": "1FTFW1ET3DFB07826", "dtc_code": "P0420", "event_timestamp": "2024-02-20 08:32:10"},
        ])
        result = check_duplicate_dtc_events(df)
        assert result["passed"] is True

    def test_detects_firmware_duplicates(self):
        df = pd.DataFrame([
            {"vin": "3FADP4BJ5EM239018", "dtc_code": "P0420", "event_timestamp": "2024-01-09 09:06:30"},
            {"vin": "3FADP4BJ5EM239018", "dtc_code": "P0420", "event_timestamp": "2024-01-09 09:06:30"},
        ])
        result = check_duplicate_dtc_events(df)
        assert result["passed"] is False
        assert result["records_failed"] == 1
