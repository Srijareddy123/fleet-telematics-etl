"""
Postgres connection for the FastAPI service.

Using psycopg2 directly here instead of an ORM, the API is strictly read-only
and the queries are simple enough that an ORM would just add ceremony.
"""
import os
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


def _get_dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'fleet_dw')} "
        f"user={os.getenv('POSTGRES_USER', 'etl_user')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'etl_password')}"
    )


@contextmanager
def get_cursor() -> Iterator[psycopg2.extensions.cursor]:
    """Open a read-only cursor, yield it, then close everything."""
    conn = psycopg2.connect(_get_dsn())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()
