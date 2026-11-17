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
    source_league_id_overrides: dict[str, str] | None = None,
) -> list[dict]:
    """Fetch all teams for each league in league_ids.

    uid is always constructed as "{queried_league_id}-{team_id}" — the
    primary key used throughout the pipeline and database. This is
    deliberately keyed off the league you asked for, NOT the per-team
    "idLeague" field TheSportsDB returns in the response.

    Why: the same physical team can play in more than one league (e.g. a
    club's domestic league and an international competition like
    EuroLeague Women). Each such membership is meant to be a distinct
    asset row with its own uid, stats, and Elo history — so a team
    fetched under league 4892 always becomes uid "4892-<team_id>", even
    if TheSportsDB's response tags that team with a different "idLeague"
    (which for some aggregate/multi-country leagues reflects the team's
    domestic league instead of the league actually queried). A mismatch
    is logged for visibility but never changes how the row is keyed.

    Args:
        client: rate-limited API client
        league_ids: league IDs to fetch teams for (these become league_id/
            uid on every row, regardless of what the API reports)
        source_league_id_overrides: optional {league_id -> source_league_id}
            map. When a league_id has an override, teams are fetched from
            TheSportsDB using the override's league ID (in case the
            league's own teams endpoint returns nothing usable), but the
            resulting rows are still keyed under the original league_id.
            Set per-league via admin.league_registry.team_source_league_id.

    Returns list of dicts suitable for upsert into api_assets.
    """
    teams: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()
    overrides = source_league_id_overrides or {}

    for league_id in league_ids:
        league_id_str = str(league_id)
        fetch_id = overrides.get(league_id_str) or league_id
        if str(fetch_id) != league_id_str:
            logger.info(
                "Teams for league %s: using source override, fetching from league %s.",
                league_id_str, fetch_id,
            )

        url = teams_url(fetch_id)
        try:
            data = client.get(url)
        except httpx.HTTPError as exc:
            logger.warning("Failed teams for league %s: %s", league_id_str, exc)
            continue

        team_list = data.get("list") or []
        if not team_list:
            logger.warning("No teams returned for league %s.", league_id_str)
            continue

        for item in team_list:
            team_id = item.get("idTeam")
            if not team_id:
                continue

            api_league_id = item.get("idLeague")
            if api_league_id is not None and str(api_league_id) != league_id_str:
                logger.warning(
                    "Team %s (%s): API tagged idLeague=%s but keying under "
                    "queried league_id=%s (uid=%s-%s).",
                    team_id, item.get("strTeam"), api_league_id,
                    league_id_str, league_id_str, team_id,
                )

            teams.append({
                "uid": f"{league_id_str}-{team_id}",
                "league_id": league_id_str,
                "team_name": item.get("strTeam"),
                "team_short": item.get("strTeamShort") or "",
                "team_logo": item.get("strBadge") or "",
                "team_country": item.get("strCountry") or "",
                "created_at": now,
                "updated_at": now,
            })

    logger.info("Fetched %d teams.", len(teams))
    return teams
