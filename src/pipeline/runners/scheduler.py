"""Automatic league activation based on upcoming events.

Checks api.events for whitelisted-but-inactive leagues that have events
within a configurable lookahead window.  Auto-activates them so the
daily pipeline picks them up.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from supabase import Client

logger = logging.getLogger(__name__)

DEFAULT_LOOKAHEAD_DAYS = 14


def auto_activate_leagues(
    supabase: Client,
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS,
) -> list[dict]:
    """Find and activate whitelisted leagues with upcoming events.

    Queries api.events for whitelisted-but-inactive leagues that have
    at least one event within the next *lookahead_days* days.

    Returns:
        List of dicts ``{"league_id": ..., "league_name": ...}`` for each
        activated league.
    """
    # 1. Get whitelisted-but-inactive leagues that have sport_type set
    registry_rows = (
        supabase.schema("admin")
        .table("league_registry")
        .select("league_id,league_name,sport_type")
        .eq("is_whitelisted", True)
        .eq("is_active", False)
        .not_.is_("sport_type", "null")
        .execute()
        .data
    ) or []

    if not registry_rows:
        return []

    candidate_ids = [str(r["league_id"]) for r in registry_rows]
    candidate_names = {str(r["league_id"]): r["league_name"] for r in registry_rows}

    # 2. Check api.events for upcoming events in those leagues
    today = datetime.now(timezone.utc).date().isoformat()
    future = (datetime.now(timezone.utc).date() + timedelta(days=lookahead_days)).isoformat()

    upcoming = (
        supabase.schema("api")
        .table("events")
        .select("league_id")
        .in_("league_id", candidate_ids)
        .gte("event_date", today)
        .lte("event_date", future)
        .limit(1000)
        .execute()
        .data
    ) or []

    leagues_with_events = {str(e["league_id"]) for e in upcoming}

    if not leagues_with_events:
        logger.info(
            "Auto-activate: no candidate leagues have events in the next %d days.",
            lookahead_days,
        )
        return []

    # 3. Activate those leagues
    activated: list[dict] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for lid in sorted(leagues_with_events):
        name = candidate_names.get(lid, "Unknown")
        supabase.schema("admin").table("league_registry").update({
            "is_active": True,
            "updated_at": now_iso,
            "notes": f"Auto-activated {datetime.now(timezone.utc).date()} — events within {lookahead_days} days",
        }).eq("league_id", lid).execute()
        logger.info("Auto-activate: activated %s (%s).", name, lid)
        activated.append({"league_id": lid, "league_name": name})

    return activated
