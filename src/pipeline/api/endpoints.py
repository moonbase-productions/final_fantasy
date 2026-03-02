from __future__ import annotations

THESPORTSDB_API_BASE_URL = "https://www.thesportsdb.com/api/v2/json"


def all_leagues_url() -> str:
    """All leagues in TheSportsDB."""
    return f"{THESPORTSDB_API_BASE_URL}/all/leagues"


def league_detail_url(league_id: int | str) -> str:
    """Extended metadata for a single league."""
    return f"{THESPORTSDB_API_BASE_URL}/lookup/league/{league_id}"


def seasons_url(league_id: int | str) -> str:
    """All seasons for a single league."""
    return f"{THESPORTSDB_API_BASE_URL}/list/seasons/{league_id}"


def teams_url(league_id: int | str) -> str:
    """All teams in a single league."""
    return f"{THESPORTSDB_API_BASE_URL}/list/teams/{league_id}"


def season_events_url(league_id: int | str, season: str) -> str:
    """All events for a specific league-season combination.

    Season format examples: '2024-2025', '2024', 'Season 2024'
    """
    return f"{THESPORTSDB_API_BASE_URL}/filter/events/{league_id}/{season}"
