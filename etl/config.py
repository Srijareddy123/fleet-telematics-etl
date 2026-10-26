"""
Centralised config: reads from environment variables.

Everything downstream imports from here instead of calling os.getenv() directly.
That way if we need to change a variable name we change it in one place.
"""
import os

# ── PostgreSQL ────────────────────────────────────────────────────────────────
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "fleet_dw")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "etl_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")

POSTGRES_DSN = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ── MinIO / S3 ────────────────────────────────────────────────────────────────
MINIO_ENDPOINT        = os.getenv("MINIO_ENDPOINT",        "localhost:9000")
MINIO_ACCESS_KEY      = os.getenv("MINIO_ACCESS_KEY",      "minioadmin")
MINIO_SECRET_KEY      = os.getenv("MINIO_SECRET_KEY",      "minioadmin123")
MINIO_RAW_BUCKET      = os.getenv("MINIO_RAW_BUCKET",      "etl-raw-data")
MINIO_PROCESSED_BUCKET = os.getenv("MINIO_PROCESSED_BUCKET", "etl-processed")

# Set MINIO_SECURE=true when pointing at a TLS endpoint such as S3
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# ── Pipeline settings ─────────────────────────────────────────────────────────
# How many times to retry a failing MinIO download before giving up.
# 3 is enough for transient network blips; we don't want to mask real errors.
MAX_DOWNLOAD_RETRIES = int(os.getenv("MAX_DOWNLOAD_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = int(os.getenv("RETRY_BACKOFF_SECONDS", "5"))

# ── Telemetry data quality thresholds ────────────────────────────────────────
# Plausible sensor ranges. Anything outside them is flagged as an outlier
# (is_outlier = TRUE) rather than dropped. Override through environment variables.
SPEED_MAX_MPH          = float(os.getenv("SPEED_MAX_MPH",          "250"))
RPM_MAX                = float(os.getenv("RPM_MAX",                 "8000"))
FUEL_LEVEL_MIN_PCT     = float(os.getenv("FUEL_LEVEL_MIN_PCT",      "0"))
FUEL_LEVEL_MAX_PCT     = float(os.getenv("FUEL_LEVEL_MAX_PCT",      "100"))
OIL_TEMP_MIN_F         = float(os.getenv("OIL_TEMP_MIN_F",          "100"))
OIL_TEMP_MAX_F         = float(os.getenv("OIL_TEMP_MAX_F",          "300"))
BATTERY_VOLTAGE_MIN    = float(os.getenv("BATTERY_VOLTAGE_MIN",     "9.0"))
BATTERY_VOLTAGE_MAX    = float(os.getenv("BATTERY_VOLTAGE_MAX",     "16.0"))
