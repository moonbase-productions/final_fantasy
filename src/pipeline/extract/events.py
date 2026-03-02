from __future__ import annotations
import logging
from datetime import datetime, timedelta, timezone

import httpx
from supabase import Client

from pipeline.api.client import RateLimitedClient
from pipeline.api.endpoints import season_events_url
from pipeline.transform.normalize import (
    sanitize_date,
    sanitize_score,
    sanitize_time,
)

logger = logging.getLogger(__name__)


def _is_season_complete(supabase: Client, league_id: int, season: str) -> bool:
    """Return True if all scoreable events in this season are already recorded.

    A season is 'complete' when there are zero events that are:
    - older than 1 day (so the score is finalised), AND
    - missing a score

    If the season has no events at all in the DB, returns False (must fetch).
    """
    cutoff_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

    # Try RPC first (fast path). Falls back to direct query if RPC unavailable.
    try:
        rpc = supabase.rpc(
            "count_pending_events",
            {"p_league_id": str(league_id), "p_season": season},
        ).execute()

        pending: int | None = None
        if isinstance(rpc.data, int):
            logger.debug("RPC returned int directly: %s", rpc.data)
            pending = rpc.data
        elif isinstance(rpc.data, list) and rpc.data:
            first = rpc.data[0]
            if isinstance(first, dict):
                for key in ("count_pending_events", "pending", "count"):
                    if key in first:
                        logger.debug("RPC returned list[dict] with key=%s", key)
                        pending = int(first[key])
                        break
        elif isinstance(rpc.data, dict):
            for key in ("count_pending_events", "pending", "count"):
                if key in rpc.data:
                    logger.debug("RPC returned dict with key=%s", key)
                    pending = int(rpc.data[key])
                    break

        if pending is None:
            logger.debug("RPC response format unrecognized: %r", rpc.data)

        if pending is not None:
            # Still check if we have any events at all.
            total_response = (
                supabase.schema("api").table("events")
                .select("event_id", count="exact")
                .eq("league_id", str(league_id))
                .eq("league_season", season)
                .execute()
            )
            total = total_response.count or 0
            if total == 0:
                return False
            return pending == 0
    except Exception as exc:
        logger.info("RPC completeness check unavailable; falling back to query: %s", exc)

    # Direct query fallback:
    try:
        response = (
            supabase.schema("api").table("events")
            .select("event_id", count="exact")
            .eq("league_id", str(league_id))
            .eq("league_season", season)
            .is_("team_score_home", "null")
            .lt("event_date", cutoff_date)
            .execute()
        )
        pending = response.count or 0

        # Also check if we have any events at all
        total_response = (
            supabase.schema("api").table("events")
            .select("event_id", count="exact")
            .eq("league_id", str(league_id))
            .eq("league_season", season)
            .execute()
        )
        total = total_response.count or 0

        if total == 0:
            return False  # No events yet — must fetch
        return pending == 0
    except Exception as exc:
        logger.warning("Could not check season completeness: %s", exc)
        return False  # When in doubt, fetch


def _parse_events(
    data: dict,
    league_id: int,
    season: str,
) -> list[dict]:
    """Parse raw API event response into a list of dicts for api_events."""
    # API v2 returns results under "filter"; fall back to "events" for older responses.
    events = data.get("filter") or data.get("events") or []
    parsed: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for item in events:
        event_id = item.get("idEvent")
        if not event_id:
            continue

        league_id_str = item.get("idLeague") or str(league_id)
        home_id = item.get("idHomeTeam") or ""
        away_id = item.get("idAwayTeam") or ""

        # Skip events without team assignments (e.g. TBD fixtures)
        if not home_id or not away_id:
            continue

        record = {
            "event_id": event_id,
            "event_date": sanitize_date(item.get("dateEvent") or "1970-01-01"),
            "event_time": sanitize_time(item.get("strTime") or ""),
            "league_id": league_id_str,
            "league_sport": item.get("strSport") or "",
            "league_season": season,
            "league_round": str(item.get("intRound") or ""),
            "uid_home": f"{league_id_str}-{home_id}",
            "uid_away": f"{league_id_str}-{away_id}",
            "team_score_home": sanitize_score(item.get("intHomeScore")),
            "team_score_away": sanitize_score(item.get("intAwayScore")),
            "event_status": item.get("strStatus") or "",
            "event_video": item.get("strVideo") or "",
            "updated_at": now,
        }
        # Capture finish position if present (multi-competitor sports)
        result_val = item.get("intResult")
        if result_val is not None and result_val != "":
            try:
                record["finish_position"] = int(result_val)
            except (ValueError, TypeError):
                pass
        parsed.append(record)

    return parsed


def fetch_events_for_season(
    client: RateLimitedClient,
    league_id: int,
    season: str,
) -> list[dict]:
    """Fetch all events for a single league-season from the API."""
    url = season_events_url(league_id, season)
    try:
        data = client.get(url)
    except httpx.HTTPError as exc:
        logger.warning(
            "Failed to fetch events for league %s season %s: %s",
            league_id, season, exc,
        )
        return []

    events = _parse_events(data, league_id, season)
    logger.info(
        "Fetched %d events for league %s season %s.",
        len(events), league_id, season,
    )
    return events


def fetch_events_incremental(
    client: RateLimitedClient,
    supabase: Client,
    whitelisted_ids: list[int],
    season_last5: list[dict],
    current_only: bool = False,
) -> list[dict]:
    """Fetch events using incremental skip strategy.

    For each (league_id, season) pair in season_last5:
    - season_rank == 1 (current): always fetch
    - season_rank >= 2 (past): skip if all scoreable events already recorded

    If current_only=True, only fetch season_rank==1 (used by daily runner).

    Args:
        client: rate-limited API client
        supabase: supabase client for completeness checks
        whitelisted_ids: list of league IDs to process
        season_last5: rows from sql_season_last5 table
        current_only: if True, only fetch current season

    Returns:
        Flat list of event dicts for upsert into api_events.
    """
    all_events: list[dict] = []
    whitelisted_set = set(str(i) for i in whitelisted_ids)

    for row in season_last5:
        league_id = row["league_id"]
        season = row["league_season"]
        try:
            rank = int(row["season_rank"])
        except (ValueError, TypeError):
            logger.warning("Invalid season_rank for league %s season %s; skipping.", league_id, season)
            continue

        if str(league_id) not in whitelisted_set:
            continue

        if current_only and rank != 1:
            continue

        if rank >= 2 and _is_season_complete(supabase, league_id, season):
            logger.info(
                "Skipping complete past season: league %s season %s.",
                league_id, season,
            )
            continue

        events = fetch_events_for_season(client, int(league_id), season)
        all_events.extend(events)

    logger.info("Total events fetched (incremental): %d", len(all_events))
    return all_events
