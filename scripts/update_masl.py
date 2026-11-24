"""Load MASL (league 9001) from its season snapshots in custom/masl/.

Upserts the league, league details, seasons, registry row (whitelisted +
active), teams (api.assets + api.team_details, with logos) and games, then
rebuilds stats and the derived tables, same as update_ncaa_womens_soccer.py.

Run: python scripts/update_masl.py            # load + rebuild
     python scripts/update_masl.py --dry-run  # print what would be loaded
     python scripts/update_masl.py --no-rebuild  # load only

To add a season: export its Scores page to custom/masl/scores_<season>.json
(same shape as scores_2025-2026.json) and re-run.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import Counter
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import validate_runtime_settings
from pipeline.db import get_supabase_client, get_pg_connection
from pipeline.extract.masl import (
    build_masl_rows,
    LEAGUE_ID as MASL_LEAGUE_ID,
    LEAGUE_NAME as MASL_LEAGUE_NAME,
    LEAGUE_SPORT as MASL_LEAGUE_SPORT,
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
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Build rows and print a summary; write nothing.")
    parser.add_argument("--no-rebuild", action="store_true", help="Skip the stats + derived-table rebuild.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_time = time.monotonic()

    if args.dry_run:
        rows = build_masl_rows(supabase=None)
        for table, recs in rows.items():
            print(f"{table}: {len(recs)} rows")
        for a in rows["assets"]:
            print(f"  {a['uid']:<10} {a['team_name']:<20} {a['team_logo']}")
        print("statuses:", dict(Counter(e["event_status"] for e in rows["events"])))
        print("first event:", rows["events"][0])
        return

    validate_runtime_settings()
    supabase = get_supabase_client()
    now_iso = datetime.now(timezone.utc).isoformat()

    rows = build_masl_rows(supabase)

    batch_upsert(supabase, "api.leagues", rows["leagues"], "league_id")
    batch_upsert(supabase, "api.league_details", rows["league_details"], "league_id")
    batch_upsert(supabase, "api.seasons", rows["seasons"], "league_id,league_season")

    supabase.schema("admin").table("league_registry").upsert({
        "league_id": str(MASL_LEAGUE_ID),
        "league_name": MASL_LEAGUE_NAME,
        "league_sport": MASL_LEAGUE_SPORT,
        "sport_type": "standard",
        "display_name": "MASL",
        "is_whitelisted": True,
        "is_active": True,
        "updated_at": now_iso,
    }, on_conflict="league_id").execute()
    logger.info("Ensured admin.league_registry row for league_id=%s is active.", MASL_LEAGUE_ID)

    batch_upsert(supabase, "api.assets", rows["assets"], "uid")
    batch_upsert(supabase, "api.team_details", rows["team_details"], "team_id")
    logger.info("Upserted %d MASL assets / team_details.", len(rows["assets"]))

    events = normalize_events(rows["events"], {str(MASL_LEAGUE_ID): "standard"})
    batch_upsert(supabase, "api.events", events, "event_id")
    logger.info("Upserted %d MASL events.", len(events))

    supabase.schema("admin").table("league_registry").update({
        "last_fetched_at": now_iso,
        "team_count": len(rows["assets"]),
    }).eq("league_id", str(MASL_LEAGUE_ID)).execute()

    if not args.no_rebuild:
        with get_pg_connection() as conn:
            for sql_file in DERIVED_SQL_FILES[:5]:
                run_sql_file(conn, sql_file)

            scored_df = pd.DataFrame(paginate(supabase, "derived.events_scored"))
            stats_records, tiers_data = compute_and_prepare_stats(scored_df)
            batch_upsert(supabase, "stats.team_stats", stats_records, "uid,league_season")
            batch_upsert(supabase, "stats.team_tiers", tiers_data, "uid")
            logger.info("Upserted %d stat rows, %d tier rows.", len(stats_records), len(tiers_data))

            for sql_file in DERIVED_SQL_FILES[5:]:
                run_sql_file(conn, sql_file)
            apply_rls_all(conn)

    logger.info("MASL UPDATE COMPLETE in %.1fs", time.monotonic() - start_time)


if __name__ == "__main__":
    main()
