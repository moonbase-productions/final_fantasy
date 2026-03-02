from __future__ import annotations
from contextlib import contextmanager
from typing import Any, Generator

from supabase import create_client, Client

from pipeline.config import settings

try:
    import psycopg2
    import psycopg2.extensions
except ModuleNotFoundError:
    psycopg2 = None  # type: ignore[assignment]


def get_supabase_client() -> Client:
    """Return a Supabase PostgREST client using the service role key.

    Use this for all data upserts (api_* and py_* tables).
    The service role key bypasses Row Level Security — required for writes.
    """
    return create_client(settings.supabase_url, settings.supabase_service_role_key)


@contextmanager
def get_pg_connection() -> Generator[Any, None, None]:
    """Yield a direct psycopg2 Postgres connection for DDL statements.

    This MUST connect to the direct Supabase host (db.XXXX.supabase.co, port 5432).
    The connection pooler does not support DDL.

    Usage:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS ...")
            conn.commit()
    """
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is required for direct Postgres access. Install psycopg2-binary."
        )

    conn = psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=15,
        options="-c statement_timeout=300000",  # 5-minute max per statement
    )
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
