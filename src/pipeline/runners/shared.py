"""Shared utilities for pipeline runners.

Contains constants and helper functions used by both full_refresh and daily_update.
"""
from __future__ import annotations

import logging
import math

import pandas as pd

from pipeline.transform.stats import compute_basic_stats
from pipeline.transform.elo import compute_elo_stats
from pipeline.transform.tiers import compute_tiers
from pipeline.transform.luck import compute_luck

logger = logging.getLogger(__name__)


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


def paginate(supabase, table: str, page_size: int = 1000) -> list[dict]:
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


def compute_and_prepare_stats(
    scored_df: pd.DataFrame,
) -> tuple[list[dict], list[dict]]:
    """Compute all stats (basic, elo, tiers, luck) and prepare for upsert.

    Returns:
        Tuple of (stats_records, tiers_data) ready for batch_upsert.
    """
    if scored_df.empty:
        logger.warning("No scored events found — skipping stats computation.")
        return [], []

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

    return stats_records, tiers_data
