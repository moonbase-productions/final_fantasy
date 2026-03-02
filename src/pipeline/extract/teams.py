from __future__ import annotations
import logging
from datetime import datetime, timezone

import httpx

from pipeline.api.client import RateLimitedClient
from pipeline.api.endpoints import teams_url

logger = logging.getLogger(__name__)


def fetch_teams(
    client: RateLimitedClient,
    league_ids: list[int],
) -> list[dict]:
    """Fetch all teams for each league in league_ids.

    uid is constructed as "{league_id}-{team_id}" — the primary key
    used throughout the pipeline and database.
    Returns list of dicts suitable for upsert into api_assets.
    """
    teams: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for league_id in league_ids:
        url = teams_url(league_id)
        try:
            data = client.get(url)
        except httpx.HTTPError as exc:
            logger.warning("Failed teams for league %s: %s", league_id, exc)
            continue

        team_list = data.get("list") or []
        if not team_list:
            logger.warning("No teams returned for league %s.", league_id)
            continue

        for item in team_list:
            team_id = item.get("idTeam")
            if not team_id:
                continue
            teams.append({
                "uid": f"{item.get('idLeague')}-{team_id}",
                "league_id": item.get("idLeague"),
                "team_name": item.get("strTeam"),
                "team_short": item.get("strTeamShort") or "",
                "team_logo": item.get("strBadge") or "",
                "team_country": item.get("strCountry") or "",
                "created_at": now,
                "updated_at": now,
            })

    logger.info("Fetched %d teams.", len(teams))
    return teams
