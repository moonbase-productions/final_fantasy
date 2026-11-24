"""NCAA Division I women's basketball update (league 9002, custom source).

Registers the league (whitelisted + active), fetches games from the NCAA
scoreboard feed, upserts seasons/teams/games, then rebuilds stats and the
derived tables (same as update_ncaa_womens_soccer.py).

Run: python scripts/update_ncaa_womens_basketball.py                     # last 3 days + next 21
     python scripts/update_ncaa_womens_basketball.py --days-back 30
     python scripts/update_ncaa_womens_basketball.py --since 2023-10-25  # backfill (in-season dates only)
     python scripts/update_ncaa_womens_basketball.py --since 2023-11-01 --until 2023-11-30 --no-rebuild  # one chunk
     add --no-rebuild to skip the stats/derived rebuild
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from pipeline.config import validate_runtime_settings
from pipeline.db import get_supabase_client, get_pg_connection
from pipeline.extract.ncaa_womens_basketball import (
    fetch_ncaa_womens_basketball,
    LEAGUE_ID as WBB_LEAGUE_ID,
    LEAGUE_NAME as WBB_LEAGUE_NAME,
    LEAGUE_NAME_ALTERNATE as WBB_LEAGUE_NAME_ALTERNATE,
    LEAGUE_SPORT as WBB_LEAGUE_SPORT,
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
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--since", type=date.fromisoformat, help="Backfill from this date (YYYY-MM-DD) to today + --days-forward; skips off-season dates.")
    parser.add_argument("--until", type=date.fromisoformat, help="Last date to fetch (YYYY-MM-DD). Default: today + --days-forward.")
    parser.add_argument("--days-back", type=int, default=3, help="Days before today to (re-)fetch. Default 3.")
    parser.add_argument("--days-forward", type=int, default=21, help="Days past today to fetch. Default 21.")
    parser.add_argument("--no-rebuild", action="store_true", help="Skip the stats + derived-table rebuild.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_time = time.monotonic()
    validate_runtime_settings()
    supabase = get_supabase_client()
    now_iso = datetime.now(timezone.utc).isoformat()

    league_row = {
        "league_id": str(WBB_LEAGUE_ID),
        "league_name": WBB_LEAGUE_NAME,
        "league_sport": WBB_LEAGUE_SPORT,
        "league_name_alternate": WBB_LEAGUE_NAME_ALTERNATE,
    }
    batch_upsert(supabase, "api.leagues", [league_row], "league_id")
    supabase.schema("admin").table("league_registry").upsert({
        "league_id": str(WBB_LEAGUE_ID),
        "league_name": WBB_LEAGUE_NAME,
        "league_sport": WBB_LEAGUE_SPORT,
        "sport_type": "standard",
        "display_name": "NCAA Women's Basketball",
        "is_whitelisted": True,
        "is_active": True,
        "updated_at": now_iso,
    }, on_conflict="league_id").execute()

    today = datetime.now(timezone.utc).date()
    end = args.until or (today + timedelta(days=args.days_forward))
    start = args.since or (today - timedelta(days=args.days_back))
    logger.info("Fetch window: %s to %s%s.", start, end, " (in-season dates only)" if args.since else "")
    events, assets, team_details, seasons = fetch_ncaa_womens_basketball(
        supabase, start, end, only_in_season=bool(args.since),
    )

    if seasons:
        batch_upsert(supabase, "api.seasons",
                     [{"league_id": str(WBB_LEAGUE_ID), "league_season": s, "updated_at": now_iso} for s in seasons],
                     "league_id,league_season")
        current = max(seasons)
        batch_upsert(supabase, "api.league_details", [{
            **league_row,
            "league_division": "1",
            "league_current_season": current,
            "league_gender": "Female",
            "league_country": "United States",
        }], "league_id")
    if assets:
        batch_upsert(supabase, "api.assets", assets, "uid")
        batch_upsert(supabase, "api.team_details", team_details, "team_id")
    events = normalize_events(events, {str(WBB_LEAGUE_ID): "standard"})
    batch_upsert(supabase, "api.events", events, "event_id")
    logger.info("Upserted %d events, %d teams, seasons %s.", len(events), len(assets), seasons)

    count = (
        supabase.schema("api").table("assets").select("uid", count="exact")
        .eq("league_id", str(WBB_LEAGUE_ID)).execute().count
    )
    supabase.schema("admin").table("league_registry").update({
        "last_fetched_at": now_iso, "team_count": int(count or 0),
    }).eq("league_id", str(WBB_LEAGUE_ID)).execute()

    if not args.no_rebuild:
        with get_pg_connection() as conn:
            for sql_file in DERIVED_SQL_FILES[:5]:
                run_sql_file(conn, sql_file)
            stats_records, tiers_data = compute_and_prepare_stats(pd.DataFrame(paginate(supabase, "derived.events_scored")))
            batch_upsert(supabase, "stats.team_stats", stats_records, "uid,league_season")
            batch_upsert(supabase, "stats.team_tiers", tiers_data, "uid")
            for sql_file in DERIVED_SQL_FILES[5:]:
                run_sql_file(conn, sql_file)
            apply_rls_all(conn)

    logger.info("NCAA WOMEN'S BASKETBALL UPDATE COMPLETE in %.1fs", time.monotonic() - start_time)


if __name__ == "__main__":
    main()
