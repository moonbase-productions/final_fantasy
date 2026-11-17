"""Explicit, NCAA-women's-soccer-only update.

Unlike daily_update.py, this does NOT depend on admin.league_registry.is_active
being set for league 9000 — it always fetches/upserts this league. It also
ensures a registry row exists (whitelisted + active) so future full daily_update
runs pick it up automatically too.

Run: python scripts/update_ncaa_womens_soccer.py
"""
from __future__ import annotations
import argparse
import logging
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import validate_runtime_settings
from pipeline.db import get_supabase_client, get_pg_connection
from pipeline.extract.ncaa_womens_soccer import (
    fetch_ncaa_womens_soccer_incremental,
    LEAGUE_ID as NCAA_LEAGUE_ID,
    LEAGUE_NAME as NCAA_LEAGUE_NAME,
    LEAGUE_SPORT as NCAA_LEAGUE_SPORT,
)
from pipeline.transform.normalize import normalize_events
from pipeline.load.upsert import batch_upsert
from pipeline.sql.executor import run_sql_file, apply_rls_all
from pipeline.runners.shared import DERIVED_SQL_FILES, paginate, compute_and_prepare_stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days-back", type=int, default=3,
        help="How many days before today to (re-)fetch. Use a large value "
             "(e.g. days since season start) to force a full-season rescrape. "
             "Default 3 matches the normal incremental trailing window.",
    )
    parser.add_argument(
        "--days-forward", type=int, default=21,
        help="How many days past today to fetch (upcoming schedule). Default 21.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_time = time.monotonic()
    validate_runtime_settings()
    logger.info("NCAA WOMEN'S SOCCER UPDATE STARTED at %s UTC", datetime.now(timezone.utc).isoformat())

    supabase = get_supabase_client()

    # ── Step 0: make sure the registry row exists & is active ─────────────
    # (so future `daily_update` runs pick this league up automatically too)
    supabase.schema("admin").table("league_registry").upsert({
        "league_id": str(NCAA_LEAGUE_ID),
        "league_name": NCAA_LEAGUE_NAME,
        "league_sport": NCAA_LEAGUE_SPORT,
        "sport_type": "standard",
        "is_whitelisted": True,
        "is_active": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, on_conflict="league_id").execute()
    logger.info("Ensured admin.league_registry row for league_id=%s is active.", NCAA_LEAGUE_ID)

    # ── Step 1: fetch + upsert this league only ────────────────────────────
    logger.info("Fetch window: %d days back, %d days forward.", args.days_back, args.days_forward)
    events, assets, team_details = fetch_ncaa_womens_soccer_incremental(
        supabase, days_back=args.days_back, days_forward=args.days_forward,
    )

    if assets:
        batch_upsert(supabase, "api.assets", assets, "uid")
        batch_upsert(supabase, "api.team_details", team_details, "team_id")
        logger.info("Upserted %d assets / team_details.", len(assets))

    normalized_events = normalize_events(events, {str(NCAA_LEAGUE_ID): "standard"})
    batch_upsert(supabase, "api.events", normalized_events, "event_id")
    logger.info("Upserted %d events.", len(normalized_events))

    # ── Step 2: rebuild derived tables + stats (same as daily_update) ──────
    with get_pg_connection() as conn:
        for sql_file in DERIVED_SQL_FILES[:5]:
            run_sql_file(conn, sql_file)

        scored_rows = paginate(supabase, "derived.events_scored")
        scored_df = pd.DataFrame(scored_rows)
        stats_records, tiers_data = compute_and_prepare_stats(scored_df)
        batch_upsert(supabase, "stats.team_stats", stats_records, "uid,league_season")
        batch_upsert(supabase, "stats.team_tiers", tiers_data, "uid")
        logger.info("Upserted %d stat rows, %d tier rows.", len(stats_records), len(tiers_data))

        for sql_file in DERIVED_SQL_FILES[5:]:
            run_sql_file(conn, sql_file)

        apply_rls_all(conn)

    logger.info("NCAA WOMEN'S SOCCER UPDATE COMPLETE in %.1fs", time.monotonic() - start_time)


if __name__ == "__main__":
    main()
