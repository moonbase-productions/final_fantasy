"""Single-league onboarding for newly activated leagues.

Called by daily_update to bootstrap leagues that have been activated
but never gone through a full_refresh.  Fetches seasons, teams, and
events for just the new league(s).

The caller is responsible for the full stats / SQL rebuild afterwards.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
from supabase import Client

from pipeline.api.client import RateLimitedClient
from pipeline.extract.events import fetch_events_incremental
from pipeline.extract.seasons import fetch_seasons
from pipeline.extract.teams import fetch_teams
from pipeline.load.upsert import batch_upsert
from pipeline.runners.shared import paginate
from pipeline.sql.executor import run_sql_file
from pipeline.transform.normalize import normalize_events

logger = logging.getLogger(__name__)


def detect_new_leagues(supabase: Client) -> list[dict]:
    """Return active leagues that have never been fetched (last_fetched_at IS NULL)."""
    rows = (
        supabase.schema("admin")
        .table("league_registry")
        .select("league_id,sport_type,league_name")
        .eq("is_active", True)
        .is_("last_fetched_at", "null")
        .execute()
        .data
    )
    return rows or []


def onboard_leagues(
    client: RateLimitedClient,
    supabase: Client,
    conn: object,
    new_leagues: list[dict],
    sport_type_map: dict[str, str],
) -> None:
    """Bootstrap data for newly activated leagues.

    Steps:
        1. Fetch seasons → upsert to api.seasons
        2. Rebuild SQL 01 + 02 (season_last5 includes new league)
        3. Fetch teams → upsert to api.assets
        4. Fetch events (all last-5 seasons) → normalize → upsert to api.events
        5. Update league_registry metadata (last_fetched_at, team_count)
    """
    if not new_leagues:
        return

    league_ids = [int(r["league_id"]) for r in new_leagues]
    league_id_strs = [str(lid) for lid in league_ids]
    names = {str(r["league_id"]): r.get("league_name", "?") for r in new_leagues}

    logger.info(
        "Onboarding %d new league(s): %s",
        len(league_ids),
        ", ".join(f"{names[str(lid)]} ({lid})" for lid in league_ids),
    )

    # 1. Seasons
    seasons = fetch_seasons(client, league_ids)
    if seasons:
        batch_upsert(supabase, "api.seasons", seasons, "league_id,league_season")
    logger.info("Onboard: upserted %d season records.", len(seasons))

    # 2. Rebuild season windows so season_last5 includes the new league
    run_sql_file(conn, "01_leagues_current.sql")
    run_sql_file(conn, "02_season_windows.sql")

    # 3. Teams
    teams = fetch_teams(client, league_ids)
    if teams:
        batch_upsert(supabase, "api.assets", teams, "uid")
    logger.info("Onboard: upserted %d teams.", len(teams))

    # 4. Events — fetch all last-5 seasons (not just current) for full Elo history
    season_last5 = paginate(supabase, "derived.season_last5")
    events_raw = fetch_events_incremental(
        client, supabase, league_id_strs, season_last5, current_only=False,
    )
    normalized = normalize_events(events_raw, sport_type_map)
    if normalized:
        batch_upsert(supabase, "api.events", normalized, "event_id")
    logger.info("Onboard: upserted %d events.", len(normalized))

    # 5. Update registry metadata
    now_iso = datetime.now(timezone.utc).isoformat()
    team_counts: dict[str, int] = {}
    if teams:
        team_counts = (
            pd.DataFrame(teams)["league_id"]
            .astype(str)
            .value_counts()
            .astype(int)
            .to_dict()
        )
    for lid in league_id_strs:
        supabase.schema("admin").table("league_registry").update({
            "last_fetched_at": now_iso,
            "team_count": int(team_counts.get(lid, 0)),
        }).eq("league_id", lid).execute()

    logger.info("Onboard: registry updated for %d league(s).", len(league_ids))
