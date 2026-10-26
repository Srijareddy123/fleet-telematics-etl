-- =============================================================================
-- Database initialisation script
-- Runs automatically when the Postgres container starts for the first time.
--
-- Creates:
--   1. airflow db: Airflow stores its own metadata here (DAG runs, task logs)
--   2. fleet_dw: Our data warehouse; all ETL output lands here
-- =============================================================================

-- ── Airflow metadata database ─────────────────────────────────────────────
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- ── Fleet data warehouse ──────────────────────────────────────────────────
CREATE USER etl_user WITH PASSWORD 'etl_password';
CREATE DATABASE fleet_dw OWNER etl_user;
GRANT ALL PRIVILEGES ON DATABASE fleet_dw TO etl_user;

\connect fleet_dw

GRANT ALL ON SCHEMA public TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES    TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO etl_user;

-- =============================================================================
-- PIPELINE AUDIT TABLE
-- Every pipeline run writes a row here so we can track history and surface
-- status through the FastAPI service.
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                  SERIAL       PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,    -- 'telemetry' | 'dtc_events' | 'fleet_vehicles'
    dag_run_id          VARCHAR(255),             -- Airflow's logical run ID
    status              VARCHAR(20)  NOT NULL,    -- running | success | failed
    records_extracted   INTEGER      DEFAULT 0,
    records_validated   INTEGER      DEFAULT 0,
    records_loaded      INTEGER      DEFAULT 0,
    records_rejected    INTEGER      DEFAULT 0,   -- rows that failed validation checks
    source_file         VARCHAR(500),             -- MinIO object path that was processed
    error_message       TEXT,
    start_time          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time            TIMESTAMP,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pipeline_runs_name       ON pipeline_runs (pipeline_name);
CREATE INDEX idx_pipeline_runs_start_time ON pipeline_runs (start_time DESC);

-- =============================================================================
-- DATA QUALITY LOG
-- One row per validation check. Gives us a paper trail when the data team asks
-- "why did 12 rows get dropped last Tuesday?"
-- =============================================================================

CREATE TABLE IF NOT EXISTS data_quality_logs (
    id               SERIAL       PRIMARY KEY,
    pipeline_run_id  INTEGER      REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    check_name       VARCHAR(100) NOT NULL,
    status           VARCHAR(20)  NOT NULL,    -- passed | failed | warning
    records_checked  INTEGER      DEFAULT 0,
    records_failed   INTEGER      DEFAULT 0,
    details          TEXT,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- RAW TABLES: landing zone
-- Exact copy of what arrived in MinIO. No transformations. Full audit trail.
-- Deliberately keeping all columns nullable so we never reject a batch just
-- because one sensor was missing data.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_telemetry (
    id               SERIAL  PRIMARY KEY,
    record_id        VARCHAR(50),
    vin              VARCHAR(17),
    device_id        VARCHAR(20),
    recorded_at      TIMESTAMP,
    speed_mph        NUMERIC(8, 2),
    rpm              NUMERIC(8, 1),
    fuel_level_pct   NUMERIC(5, 2),
    oil_temp_f       NUMERIC(6, 2),
    coolant_temp_f   NUMERIC(6, 2),
    battery_voltage  NUMERIC(5, 2),
    latitude         NUMERIC(10, 7),
    longitude        NUMERIC(10, 7),
    odometer_miles   NUMERIC(10, 1),
    ignition_status  VARCHAR(10),
    pipeline_run_id  INTEGER REFERENCES pipeline_runs(id),
    loaded_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_dtc_events (
    id               SERIAL  PRIMARY KEY,
    event_id         VARCHAR(20),
    vin              VARCHAR(17),
    device_id        VARCHAR(20),
    event_timestamp  TIMESTAMP,
    dtc_code         VARCHAR(10),
    dtc_description  TEXT,
    -- NOTE: severity came in as integers (1/2/3) from old firmware and as strings
    -- (low/medium/high) from new firmware. Storing as text here and normalising downstream.
    severity         VARCHAR(20),
    is_active        VARCHAR(10),    -- comes in as 'true'/'false' string, not boolean
    cleared_at       TIMESTAMP,
    mileage_at_event NUMERIC(10, 1),
    pipeline_run_id  INTEGER REFERENCES pipeline_runs(id),
    loaded_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_fleet_vehicles (
    id              SERIAL  PRIMARY KEY,
    vin             VARCHAR(17),
    make            VARCHAR(50),
    model           VARCHAR(50),
    year            INTEGER,
    license_plate   VARCHAR(20),
    plate_state     VARCHAR(5),
    fleet_id        VARCHAR(30),
    driver_id       VARCHAR(20),
    vehicle_type    VARCHAR(30),
    status          VARCHAR(20),
    assigned_date   DATE,
    region          VARCHAR(50),
    pipeline_run_id INTEGER REFERENCES pipeline_runs(id),
    loaded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- CURATED TABLES: clean, validated data ready for analytics
-- =============================================================================

-- One row per second per vehicle. Query-optimised with indexes on the columns
-- the analytics team filters most: vin, time range, and speed outlier detection.
CREATE TABLE IF NOT EXISTS fact_telemetry (
    telemetry_key    SERIAL   PRIMARY KEY,
    record_id        VARCHAR(50) UNIQUE NOT NULL,
    vin              VARCHAR(17) NOT NULL,
    device_id        VARCHAR(20),
    recorded_at      TIMESTAMP   NOT NULL,
    speed_mph        NUMERIC(8, 2),
    rpm              NUMERIC(8, 1),
    fuel_level_pct   NUMERIC(5, 2),
    oil_temp_f       NUMERIC(6, 2),
    coolant_temp_f   NUMERIC(6, 2),
    battery_voltage  NUMERIC(5, 2),
    latitude         NUMERIC(10, 7),
    longitude        NUMERIC(10, 7),
    odometer_miles   NUMERIC(10, 1),
    ignition_status  VARCHAR(10),
    -- Derived flag: set by the transform step when a sensor reading looks like hardware noise
    is_outlier       BOOLEAN     DEFAULT FALSE,
    updated_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_telemetry_vin         ON fact_telemetry (vin);
CREATE INDEX idx_fact_telemetry_recorded_at ON fact_telemetry (recorded_at DESC);

-- DTC events after deduplication and severity normalisation
CREATE TABLE IF NOT EXISTS fact_dtc_events (
    dtc_key          SERIAL    PRIMARY KEY,
    event_id         VARCHAR(20) UNIQUE NOT NULL,
    vin              VARCHAR(17) NOT NULL,
    device_id        VARCHAR(20),
    event_timestamp  TIMESTAMP   NOT NULL,
    dtc_code         VARCHAR(10) NOT NULL,
    dtc_description  TEXT,
    -- Normalised: always 1/2/3 regardless of firmware version
    severity_code    SMALLINT,
    -- Human-readable label derived from severity_code
    severity_label   VARCHAR(10),   -- low | medium | high
    is_active        BOOLEAN,
    cleared_at       TIMESTAMP,
    mileage_at_event NUMERIC(10, 1),
    updated_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_dtc_vin       ON fact_dtc_events (vin);
CREATE INDEX idx_fact_dtc_code      ON fact_dtc_events (dtc_code);
CREATE INDEX idx_fact_dtc_is_active ON fact_dtc_events (is_active) WHERE is_active = TRUE;

-- Vehicle master data; one row per VIN
CREATE TABLE IF NOT EXISTS dim_vehicles (
    vehicle_key   SERIAL    PRIMARY KEY,
    vin           VARCHAR(17) UNIQUE NOT NULL,
    make          VARCHAR(50),
    model         VARCHAR(50),
    year          INTEGER,
    license_plate VARCHAR(20),
    plate_state   VARCHAR(5),
    fleet_id      VARCHAR(30),
    driver_id     VARCHAR(20),
    vehicle_type  VARCHAR(30),
    status        VARCHAR(20),   -- always lowercase after normalisation
    assigned_date DATE,
    region        VARCHAR(50),
    is_active     BOOLEAN     DEFAULT TRUE,
    updated_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Convenience view used by the FastAPI /pipelines endpoint
-- =============================================================================

CREATE OR REPLACE VIEW v_latest_pipeline_runs AS
SELECT DISTINCT ON (pipeline_name)
    id,
    pipeline_name,
    dag_run_id,
    status,
    records_extracted,
    records_validated,
    records_loaded,
    records_rejected,
    source_file,
    error_message,
    start_time,
    end_time,
    EXTRACT(EPOCH FROM (COALESCE(end_time, CURRENT_TIMESTAMP) - start_time))::INTEGER AS duration_seconds
FROM pipeline_runs
ORDER BY pipeline_name, start_time DESC;

GRANT SELECT ON v_latest_pipeline_runs TO etl_user;
