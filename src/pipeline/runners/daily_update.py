"""Daily events update.

Run: python -m pipeline.runners.daily_update

Fetches only current-season events for active leagues.
Recomputes all stats and rebuilds all derived tables.

New leagues that have been activated but never fetched are automatically
onboarded (seasons, teams, events) before the normal flow.

Whitelisted-but-inactive leagues with upcoming events are auto-activated.

Expected runtime: 2-5 minutes (longer when onboarding new leagues).
"""
from __future__ import annotations
import logging
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import settings, validate_runtime_settings
from pipeline.db import get_supabase_client, get_pg_connection
from pipeline.api.client import RateLimitedClient
from pipeline.extract.events import fetch_events_incremental
from pipeline.transform.normalize import normalize_events
from pipeline.load.upsert import batch_upsert
from pipeline.sql.executor import run_sql_file, apply_rls_all
from pipeline.runners.shared import DERIVED_SQL_FILES, paginate, compute_and_prepare_stats
from pipeline.runners.onboard import detect_new_leagues, onboard_leagues
from pipeline.runners.scheduler import auto_activate_leagues

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    start_time = time.monotonic()
    validate_runtime_settings()
    logger.info("DAILY UPDATE STARTED at %s UTC", datetime.now(timezone.utc).isoformat())

    supabase = get_supabase_client()

    with RateLimitedClient() as client, get_pg_connection() as conn:

        # ── Step 1: Auto-activate leagues with upcoming events ────────────
        if settings.AUTO_ACTIVATE_ENABLED:
            activated = auto_activate_leagues(
                supabase, settings.AUTO_ACTIVATE_LOOKAHEAD_DAYS,
            )
            if activated:
                logger.info(
                    "Auto-activated %d league(s): %s",
                    len(activated),
                    ", ".join(a["league_name"] for a in activated),
                )

        # ── Step 2: Read active leagues ───────────────────────────────────
        rows = (
            supabase.schema("admin").table("league_registry")
            .select("league_id,sport_type")
            .eq("is_active", True)
            .execute()
            .data
        )
        active_ids = [str(r["league_id"]) for r in rows]
        sport_type_map = {str(r["league_id"]): r["sport_type"] for r in rows}
        logger.info("Active leagues: %d", len(active_ids))

        # ── Step 3: Onboard never-fetched leagues ─────────────────────────
        new_leagues = detect_new_leagues(supabase)
        if new_leagues:
            onboard_leagues(client, supabase, conn, new_leagues, sport_type_map)

        # ── Step 4: Rebuild season windows (cheap: zero API calls) ────────
        run_sql_file(conn, "01_leagues_current.sql")
        run_sql_file(conn, "02_season_windows.sql")

        # ── Step 5: Fetch current-season events ──────────────────────────
        season_last5 = paginate(supabase, "derived.season_last5")
        events_raw = fetch_events_incremental(
            client, supabase, active_ids, season_last5, current_only=True,
        )
        normalized_events = normalize_events(events_raw, sport_type_map)
        batch_upsert(supabase, "api.events", normalized_events, "event_id")
        logger.info("Upserted %d current-season events.", len(normalized_events))

        # ── Step 6: Rebuild event tables ──────────────────────────────────
        for sql_file in ["03_web_events.sql", "04_events_scored.sql", "05_events_split.sql"]:
            run_sql_file(conn, sql_file)

        # ── Step 7: Recompute all stats from full history ─────────────────
        scored_rows = paginate(supabase, "derived.events_scored")
        scored_df = pd.DataFrame(scored_rows)
        logger.info("Loaded %d scored events for stat computation.", len(scored_df))

        stats_records, tiers_data = compute_and_prepare_stats(scored_df)

        batch_upsert(supabase, "stats.team_stats", stats_records, "uid,league_season")
        batch_upsert(supabase, "stats.team_tiers", tiers_data, "uid")
        logger.info(
            "Upserted %d stat rows, %d tier rows.",
            len(stats_records), len(tiers_data),
        )

        # ── Step 8: Rebuild all derived tables ────────────────────────────
        for sql_file in DERIVED_SQL_FILES[5:]:  # 01-05 already done
            run_sql_file(conn, sql_file)

        apply_rls_all(conn)

    logger.info("DAILY UPDATE COMPLETE in %.1fs", time.monotonic() - start_time)


if __name__ == "__main__":
    main()
