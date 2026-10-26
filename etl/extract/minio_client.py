"""
MinIO extraction helpers.

MinIO is API-compatible with AWS S3, so pointing the endpoint and
credentials at S3 should keep this code working (not tested against S3).
"""
import logging
import os
import time

from minio import Minio
from minio.error import S3Error

from etl.config import (
    MAX_DOWNLOAD_RETRIES,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_RAW_BUCKET,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    RETRY_BACKOFF_SECONDS,
)

logger = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    """Return a connected MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def list_objects(prefix: str, bucket: str = MINIO_RAW_BUCKET) -> list[str]:
    """
    Return all object keys under a given prefix.

    Example: list_objects('telemetry/') → ['telemetry/vehicle_telemetry_raw.csv', ...]
    """
    client = get_minio_client()
    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except S3Error as exc:
        raise RuntimeError(f"Could not list objects at {bucket}/{prefix}: {exc}") from exc


def get_latest_object(prefix: str, bucket: str = MINIO_RAW_BUCKET) -> str | None:
    """
    Return the key of the most recently modified object under a prefix.

    NOTE: This means if two files land between DAG runs, only the newest one
    is picked up. That is a known limitation, listed in the README.
    """
    client = get_minio_client()
    try:
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    except S3Error as exc:
        raise RuntimeError(f"Could not list objects at {bucket}/{prefix}: {exc}") from exc

    if not objects:
        return None

    # Sort by last_modified descending to find the newest file
    objects.sort(key=lambda o: o.last_modified, reverse=True)
    return objects[0].object_name


def download_file(object_key: str, local_dir: str = "/tmp", bucket: str = MINIO_RAW_BUCKET) -> str:
    """
    Download a single object from MinIO to /tmp.

    Retries up to MAX_DOWNLOAD_RETRIES times with simple linear backoff.

    NOTE: I know there is a 'tenacity' library that handles retry logic more
    elegantly, but I did not want to add another dependency for this. A simple
    for-loop is easy to read and debug during on-call incidents.
    """
    client = get_minio_client()
    local_path = os.path.join(local_dir, os.path.basename(object_key))

    last_error: Exception | None = None
    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        try:
            client.fget_object(bucket, object_key, local_path)
            logger.info("Downloaded %s → %s (attempt %d)", object_key, local_path, attempt)
            return local_path
        except S3Error as exc:
            last_error = exc
            logger.warning("Download attempt %d/%d failed: %s", attempt, MAX_DOWNLOAD_RETRIES, exc)
            if attempt < MAX_DOWNLOAD_RETRIES:
                # Simple linear backoff: 5s, 10s before final attempt
                time.sleep(attempt * RETRY_BACKOFF_SECONDS)

    raise RuntimeError(
        f"Failed to download {object_key} after {MAX_DOWNLOAD_RETRIES} attempts: {last_error}"
    )


def move_to_processed(object_key: str, processed_bucket: str) -> None:
    """
    Copy a processed file to the 'processed' bucket and delete the original.

    We do copy-then-delete instead of a move because MinIO (like S3) has no
    atomic rename/move operation. If the delete fails after a successful copy
    the file just stays in the raw bucket, the pipeline will skip it on the
    next run because we check for the record_id in the DB before loading.
    """
    client = get_minio_client()
    filename = os.path.basename(object_key)
    destination_key = f"processed/{filename}"

    try:
        from minio.commonconfig import CopySource
        client.copy_object(
            processed_bucket,
            destination_key,
            CopySource(MINIO_RAW_BUCKET, object_key),
        )
        client.remove_object(MINIO_RAW_BUCKET, object_key)
        logger.info("Archived %s → %s/%s", object_key, processed_bucket, destination_key)
    except S3Error as exc:
        # Non-fatal: log the warning and let the pipeline continue.
        # The file just stays in raw; the idempotent load step handles duplicates.
        logger.warning("Could not archive %s: %s", object_key, exc)
