"""Full pipeline refresh.

Run: python -m pipeline.runners.full_refresh

Fetches reference data and events for all whitelisted leagues.
Computes Elo, stats, tiers, and luck.
Rebuilds all derived.* tables.

Expected runtime: 10-25 minutes depending on number of whitelisted leagues
and how many past seasons need event updates.
"""
from __future__ import annotations
import logging
import math
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import settings, validate_runtime_settings
from pipeline.db import get_supabase_client, get_pg_connection
from pipeline.api.client import RateLimitedClient
from pipeline.extract.leagues import fetch_all_leagues, fetch_league_details
from pipeline.extract.seasons import fetch_seasons
from pipeline.extract.teams import fetch_teams
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


# SQL files executed in order. Numbers in filenames enforce ordering.
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


def load_registry_snapshot(supabase) -> tuple[list[str], list[str], dict[str, str]]:
    """Read admin.league_registry and return (whitelisted_ids, active_ids, sport_type_map)."""
    rows = (
        supabase.schema("admin").table("league_registry")
        .select("league_id,sport_type,is_whitelisted,is_active")
        .eq("is_whitelisted", True)
        .execute()
        .data
    )
    whitelisted_ids: list[str] = [str(r["league_id"]) for r in rows]
    active_ids: list[str] = [str(r["league_id"]) for r in rows if r["is_active"]]
    sport_type_map: dict[str, str] = {str(r["league_id"]): r["sport_type"] for r in rows}
    return whitelisted_ids, active_ids, sport_type_map


def main() -> None:
    start_time = time.monotonic()
    validate_runtime_settings()
    logger.info("=" * 60)
    logger.info("FULL REFRESH STARTED at %s UTC", datetime.now(timezone.utc).isoformat())
    logger.info("=" * 60)

    supabase = get_supabase_client()

    with RateLimitedClient() as client, get_pg_connection() as conn:

        # ------------------------------------------------------------------
        # 1. Read league registry
        # ------------------------------------------------------------------
        whitelisted_ids, active_ids, sport_type_map = load_registry_snapshot(supabase)
        logger.info(
            "Registry: %d whitelisted, %d active leagues.",
            len(whitelisted_ids), len(active_ids),
        )

        # ------------------------------------------------------------------
        # 2. Build active-league filter tables (needed by later SQL files)
        # ------------------------------------------------------------------
        run_sql_file(conn, "01_leagues_current.sql")
        run_sql_file(conn, "02_season_windows.sql")

        # ------------------------------------------------------------------
        # 3. Extract and upsert reference data
        # ------------------------------------------------------------------
        logger.info("--- Extracting leagues ---")
        all_leagues = fetch_all_leagues(client)
        batch_upsert(supabase, "api.leagues", all_leagues, "league_id")

        logger.info("--- Extracting league details ---")
        details = fetch_league_details(client, whitelisted_ids)
        batch_upsert(supabase, "api.league_details", details, "league_id")

        logger.info("--- Extracting seasons ---")
        seasons = fetch_seasons(client, whitelisted_ids)
        batch_upsert(supabase, "api.seasons", seasons, "league_id,league_season")

        logger.info("--- Extracting teams ---")
        teams = fetch_teams(client, whitelisted_ids)
        batch_upsert(supabase, "api.assets", teams, "uid")

        # Rebuild season windows after new season data is loaded
        run_sql_file(conn, "02_season_windows.sql")

        # ------------------------------------------------------------------
        # 4. Extract events (incremental)
        # ------------------------------------------------------------------
        logger.info("--- Extracting events (incremental) ---")
        season_last5 = _paginate(supabase, "derived.season_last5")
        events_raw = fetch_events_incremental(
            client, supabase, whitelisted_ids, season_last5, current_only=False,
        )
        normalized_events = normalize_events(events_raw, sport_type_map)
        batch_upsert(supabase, "api.events", normalized_events, "event_id")

        # ------------------------------------------------------------------
        # 5. Rebuild event SQL tables
        # ------------------------------------------------------------------
        for sql_file in ["03_web_events.sql", "04_events_scored.sql", "05_events_split.sql"]:
            run_sql_file(conn, sql_file)

        # ------------------------------------------------------------------
        # 6. Compute statistics
        # ------------------------------------------------------------------
        logger.info("--- Computing statistics ---")
        scored_rows = _paginate(supabase, "derived.events_scored")
        scored_df = pd.DataFrame(scored_rows)
        logger.info("Loaded %d scored events for stat computation.", len(scored_df))

        if scored_df.empty:
            logger.warning("No scored events found — skipping stats computation.")
            stats_records: list[dict] = []
            tiers_data: list[dict] = []
        else:
            basic_df = compute_basic_stats(scored_df)
            elo_df, history_df = compute_elo_stats(scored_df)
            tiers_data = compute_tiers(elo_df)
            luck_df = compute_luck(history_df)

            # Merge all stats into one DataFrame for stats.team_stats upsert
            stats_df = elo_df.merge(basic_df, on=["uid", "league_id", "league_season"], how="outer")
            stats_df = stats_df.merge(
                luck_df[["uid", "luck_score", "luck_display"]],
                on="uid", how="left",
            )
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

        # ------------------------------------------------------------------
        # 7. Upsert computed stats
        # ------------------------------------------------------------------
        logger.info("--- Upserting stats ---")
        batch_upsert(supabase, "stats.team_stats", stats_records, "uid,league_season")
        batch_upsert(supabase, "stats.team_tiers", tiers_data, "uid")
        logger.info(
            "Upserted %d stat rows, %d tier rows.",
            len(stats_records), len(tiers_data),
        )

        # ------------------------------------------------------------------
        # 8. Rebuild all remaining derived SQL tables
        # ------------------------------------------------------------------
        logger.info("--- Rebuilding derived tables ---")
        for sql_file in DERIVED_SQL_FILES[5:]:  # 01-05 already done
            run_sql_file(conn, sql_file)

        # ------------------------------------------------------------------
        # 9. Apply RLS to all ephemeral tables
        # ------------------------------------------------------------------
        apply_rls_all(conn)

        # ------------------------------------------------------------------
        # 10. Update registry metadata
        # ------------------------------------------------------------------
        logger.info("--- Updating league registry metadata ---")
        team_counts: dict[str, int] = {}
        if teams:
            team_counts = (
                pd.DataFrame(teams)["league_id"]
                .astype(str)
                .value_counts()
                .astype(int)
                .to_dict()
            )
        now_iso = datetime.now(timezone.utc).isoformat()
        for lid in whitelisted_ids:
            supabase.schema("admin").table("league_registry").update({
                "last_fetched_at": now_iso,
                "team_count": int(team_counts.get(str(lid), 0)),
            }).eq("league_id", str(lid)).execute()

    elapsed = time.monotonic() - start_time
    logger.info("=" * 60)
    logger.info("FULL REFRESH COMPLETE in %.1fs", elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
