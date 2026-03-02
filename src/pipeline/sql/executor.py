from __future__ import annotations
import logging
from pathlib import Path
from typing import Any

try:
    import psycopg2.extensions
except ModuleNotFoundError:
    psycopg2 = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "queries"

# All ephemeral derived tables that need RLS applied after each run.
# Uses schema-qualified names: "derived.<table>"
RLS_TABLES = [
    "derived.leagues_current",
    "derived.season_current",
    "derived.season_past",
    "derived.season_last5",
    "derived.web_events",
    "derived.events_scored",
    "derived.team_stats_current",
    "derived.team_stats_previous",
    "derived.events_future_elos",
    "derived.assets_future",
    "derived.assets_stats_at",
    "derived.forecast",
    "derived.events_split",
    "derived.asset_last_10",
    "derived.assets_season_to_date",
    "derived.ref_elo",
    "derived.current_elo",
    "derived.wld",
    "derived.web_assets",
    "derived.web_assets_info",
    "derived.league_info",
]


def run_sql_file(
    conn: Any,
    filename: str,
) -> None:
    """Read a .sql file from sql/queries/ and execute it.

    Each file handles its own DROP TABLE IF EXISTS and CREATE TABLE AS SELECT.

    Args:
        conn: open psycopg2 connection (direct, not pooler)
        filename: filename only, e.g. '03_web_events.sql'
    """
    sql_path = SQL_DIR / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql = sql_path.read_text(encoding="utf-8")
    logger.info("Executing %s ...", filename)
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("Completed %s.", filename)


def apply_rls(
    conn: Any,
    qualified_table: str,
) -> None:
    """Enable RLS and create a public-read policy on a table.

    Idempotent: drops existing policy before recreating.
    Safe to call on every run.

    Args:
        conn: open psycopg2 connection
        qualified_table: schema-qualified table name, e.g. 'derived.web_assets'
    """
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {qualified_table} ENABLE ROW LEVEL SECURITY;")
        cur.execute(
            f'DROP POLICY IF EXISTS "public_read" ON {qualified_table};'
        )
        cur.execute(
            f"""
            CREATE POLICY "public_read"
            ON {qualified_table} AS PERMISSIVE
            FOR SELECT TO public
            USING (true);
            """
        )
    conn.commit()


def apply_rls_all(conn: Any) -> None:
    """Apply RLS to all ephemeral derived.* tables.

    Raises RuntimeError if any table fails — RLS failures are not recoverable.
    """
    failed: list[str] = []
    for table in RLS_TABLES:
        try:
            apply_rls(conn, table)
        except Exception as exc:
            logger.error("Failed to apply RLS to %s: %s", table, exc)
            failed.append(table)
    if failed:
        raise RuntimeError(f"RLS failed for {len(failed)} table(s): {', '.join(failed)}")
    logger.info("RLS applied to %d tables.", len(RLS_TABLES))
