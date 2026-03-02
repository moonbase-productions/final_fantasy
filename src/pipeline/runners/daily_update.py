"""Daily events update.

Run: python -m pipeline.runners.daily_update

Fetches only current-season events for active leagues.
Recomputes all stats and rebuilds all derived tables.

Expected runtime: 2-5 minutes.
"""
from __future__ import annotations
import logging
import math
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import validate_runtime_settings
from pipeline.db import get_supabase_client, get_pg_connection
from pipeline.api.client import RateLimitedClient
from pipeline.extract.events import fetch_events_incremental
from pipeline.transform.normalize import normalize_events
from pipeline.transform.stats import compute_basic_stats
from pipeline.transform.elo import compute_elo_stats
from pipeline.transform.tiers import compute_tiers
from pipeline.transform.luck import compute_luck
from pipeline.load.upsert import batch_upsert
from pipeline.sql.executor import run_sql_file, apply_rls_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _paginate(supabase, table: str, page_size: int = 1000) -> list[dict]:
    """Fetch all rows from a Supabase table, paginating past the 1000-row default limit."""
    rows: list[dict] = []
    offset = 0
    if "." in table:
        schema, tbl = table.split(".", 1)
        tbl_ref = supabase.schema(schema).table(tbl)
    else:
        tbl_ref = supabase.table(table)
    while True:
        batch = tbl_ref.select("*").range(offset, offset + page_size - 1).execute().data
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


DERIVED_SQL_FILES = [
    "01_leagues_current.sql",
    "02_season_windows.sql",
    "03_web_events.sql",
    "04_events_scored.sql",
    "05_events_split.sql",
    "06_asset_last_10.sql",
    "07_events_future_elos.sql",
    "08_assets_future.sql",
    "09_assets_stats_at.sql",
    "10_forecast.sql",
    "11_assets_season_to_date.sql",
    "12_ref_elo.sql",
    "13_current_elo.sql",
    "14_wld.sql",
    "15_web_assets.sql",
    "16_web_assets_info.sql",
    "17_league_info.sql",
]


def main() -> None:
    start_time = time.monotonic()
    validate_runtime_settings()
    logger.info("DAILY UPDATE STARTED at %s UTC", datetime.now(timezone.utc).isoformat())

    supabase = get_supabase_client()

    with RateLimitedClient() as client, get_pg_connection() as conn:

        # Read active leagues and their sport types
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

        # Rebuild season windows (cheap: zero API calls)
        run_sql_file(conn, "01_leagues_current.sql")
        run_sql_file(conn, "02_season_windows.sql")

        # Fetch current-season events only
        season_last5 = _paginate(supabase, "derived.season_last5")
        events_raw = fetch_events_incremental(
            client, supabase, active_ids, season_last5, current_only=True,
        )
        normalized_events = normalize_events(events_raw, sport_type_map)
        batch_upsert(supabase, "api.events", normalized_events, "event_id")
        logger.info("Upserted %d current-season events.", len(normalized_events))

        # Rebuild event tables
        for sql_file in ["03_web_events.sql", "04_events_scored.sql", "05_events_split.sql"]:
            run_sql_file(conn, sql_file)

        # Recompute all stats from full history
        scored_rows = _paginate(supabase, "derived.events_scored")
        scored_df = pd.DataFrame(scored_rows)
        logger.info("Loaded %d scored events for stat computation.", len(scored_df))

        if scored_df.empty:
            logger.warning("No scored events found — skipping stats computation.")
            stats_records: list[dict] = []
            tiers_data: list[dict] = []
        else:
            basic_df = compute_basic_stats(scored_df)
            elo_df, hist_df = compute_elo_stats(scored_df)
            tiers_data = compute_tiers(elo_df)
            luck_df = compute_luck(hist_df)

            stats_df = elo_df.merge(basic_df, on=["uid", "league_id", "league_season"], how="outer")
            stats_df = stats_df.merge(luck_df[["uid", "luck_score", "luck_display"]], on="uid", how="left")
            stats_df["luck_display"] = stats_df["luck_display"].fillna(50).astype(int)
            # Coerce INT columns: pandas outer-merge produces float64 for nullable ints.
            # Post-process to_dict records — apply() re-casts to float when None is mixed in.
            _int_cols = {
                "wins", "losses", "draws", "games_played",
                "home_wins", "home_losses", "home_draws", "home_games_played",
                "start_rank_league", "end_rank_league", "luck_display",
            }
            stats_records = []
            for _row in stats_df.to_dict(orient="records"):
                for _col in _int_cols:
                    if _col in _row and isinstance(_row[_col], float):
                        _row[_col] = None if math.isnan(_row[_col]) else int(_row[_col])
                stats_records.append(_row)

        batch_upsert(supabase, "stats.team_stats", stats_records, "uid,league_season")
        batch_upsert(supabase, "stats.team_tiers", tiers_data, "uid")
        logger.info(
            "Upserted %d stat rows, %d tier rows.",
            len(stats_records), len(tiers_data),
        )

        # Rebuild all derived tables
        for sql_file in DERIVED_SQL_FILES[5:]:  # 01-05 already done
            run_sql_file(conn, sql_file)

        apply_rls_all(conn)

    logger.info("DAILY UPDATE COMPLETE in %.1fs", time.monotonic() - start_time)


if __name__ == "__main__":
    main()
