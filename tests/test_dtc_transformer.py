"""Tests for DTC event transformer, focuses on the tricky severity normalisation."""
import pandas as pd

from etl.transform.dtc_events import (
    deduplicate_events,
    normalise_is_active,
    normalise_severity,
)


def make_dtc_row(**overrides) -> dict:
    base = {
        "event_id":        "DTC-0001",
        "vin":             "3FADP4BJ5EM239018",
        "device_id":       "DEV-0003",
        "event_timestamp": "2024-01-09 09:06:30",
        "dtc_code":        "P0420",
        "dtc_description": "Catalyst System Efficiency Below Threshold Bank 1",
        "severity":        "2",
        "is_active":       "true",
        "cleared_at":      pd.NA,
        "mileage_at_event": "78341.3",
    }
    base.update(overrides)
    return base


class TestNormaliseSeverity:
    def test_old_firmware_integer_1(self):
        df = pd.DataFrame([make_dtc_row(severity="1")])
        result = normalise_severity(df)
        assert result["severity_code"].iloc[0] == 1
        assert result["severity_label"].iloc[0] == "low"

    def test_old_firmware_integer_2(self):
        df = pd.DataFrame([make_dtc_row(severity="2")])
        result = normalise_severity(df)
        assert result["severity_code"].iloc[0] == 2
        assert result["severity_label"].iloc[0] == "medium"

    def test_new_firmware_string_high(self):
        df = pd.DataFrame([make_dtc_row(severity="high")])
        result = normalise_severity(df)
        assert result["severity_code"].iloc[0] == 3
        assert result["severity_label"].iloc[0] == "high"

    def test_new_firmware_string_mixed_case(self):
        df = pd.DataFrame([make_dtc_row(severity="MEDIUM")])
        result = normalise_severity(df)
        assert result["severity_code"].iloc[0] == 2

    def test_null_severity_stays_null(self):
        df = pd.DataFrame([make_dtc_row(severity=pd.NA)])
        result = normalise_severity(df)
        assert pd.isna(result["severity_code"].iloc[0])

    def test_unknown_severity_becomes_null(self):
        df = pd.DataFrame([make_dtc_row(severity="URGENT")])
        result = normalise_severity(df)
        assert pd.isna(result["severity_code"].iloc[0])


class TestNormaliseIsActive:
    def test_true_string_becomes_true(self):
        df = pd.DataFrame([make_dtc_row(is_active="true")])
        result = normalise_is_active(df)
        assert bool(result["is_active"].iloc[0])

    def test_false_string_becomes_false(self):
        df = pd.DataFrame([make_dtc_row(is_active="false")])
        result = normalise_is_active(df)
        assert not result["is_active"].iloc[0]

    def test_uppercase_true_handled(self):
        df = pd.DataFrame([make_dtc_row(is_active="TRUE")])
        result = normalise_is_active(df)
        assert bool(result["is_active"].iloc[0])


class TestDeduplicateEvents:
    def test_removes_exact_duplicate_from_old_firmware(self):
        # Reproduces the firmware duplicate delivery issue
        rows = [make_dtc_row(event_id="DTC-0005"), make_dtc_row(event_id="DTC-0005")]
        df = pd.DataFrame(rows)
        result = deduplicate_events(df)
        assert len(result) == 1

    def test_keeps_distinct_events(self):
        rows = [
            make_dtc_row(event_id="DTC-0001", dtc_code="P0420"),
            make_dtc_row(event_id="DTC-0002", dtc_code="P0300",
                         event_timestamp="2024-01-10 10:00:00"),
        ]
        df = pd.DataFrame(rows)
        result = deduplicate_events(df)
        assert len(result) == 2

    def test_same_vin_different_time_is_not_duplicate(self):
        rows = [
            make_dtc_row(event_id="DTC-0010", event_timestamp="2024-01-09 09:06:30"),
            make_dtc_row(event_id="DTC-0019", event_timestamp="2024-02-20 08:32:10"),
        ]
        df = pd.DataFrame(rows)
        result = deduplicate_events(df)
        assert len(result) == 2
