"""
Upload sample data files to MinIO.

Run this after 'make up' if you want to test the pipelines without
waiting for the minio-setup container (which runs automatically but can
sometimes fail on first boot if MinIO isn't ready fast enough).

Usage:
    python scripts/upload_to_minio.py
"""
import os
import sys
from pathlib import Path

from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
RAW_BUCKET      = os.getenv("MINIO_RAW_BUCKET", "etl-raw-data")

SAMPLE_DIR = Path(__file__).parent.parent / "sample_data"

# Map each CSV to the MinIO prefix where the DAG expects to find it
FILE_PREFIXES = {
    "vehicle_telemetry_raw.csv": "telemetry/",
    "dtc_events_raw.csv":        "dtc/",
    "fleet_vehicles_raw.csv":    "vehicles/",
}


def main():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

    # Create bucket if it doesn't exist yet
    try:
        if not client.bucket_exists(RAW_BUCKET):
            client.make_bucket(RAW_BUCKET)
            print(f"Created bucket: {RAW_BUCKET}")
    except S3Error as e:
        print(f"Error checking/creating bucket: {e}")
        sys.exit(1)

    for filename, prefix in FILE_PREFIXES.items():
        local_path = SAMPLE_DIR / filename
        if not local_path.exists():
            print(f"WARN: {local_path} not found, skipping")
            continue

        object_key = f"{prefix}{filename}"
        try:
            client.fput_object(RAW_BUCKET, object_key, str(local_path))
            print(f"Uploaded {filename} → {RAW_BUCKET}/{object_key}")
        except S3Error as e:
            print(f"ERROR uploading {filename}: {e}")
            sys.exit(1)

    print("\nDone. You can now trigger the DAGs:")
    print("  make trigger-all")
    print("  or visit http://localhost:8080 and trigger manually")


if __name__ == "__main__":
    main()
