from __future__ import annotations
import logging
from datetime import datetime, timezone

import httpx

from pipeline.api.client import RateLimitedClient
from pipeline.api.endpoints import all_leagues_url, league_detail_url

logger = logging.getLogger(__name__)


def fetch_all_leagues(client: RateLimitedClient) -> list[dict]:
    """Fetch every league in TheSportsDB.

    The response is a dict whose values are lists of league objects.
    Iterates all values and collects any dicts with 'idLeague'.
    Returns list of dicts suitable for upsert into api_leagues.
    """
    url = all_leagues_url()
    data = client.get(url)
    leagues: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for items in data.values():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict) or "idLeague" not in item:
                continue
            leagues.append({
                "league_id": item.get("idLeague"),
                "league_name": item.get("strLeague"),
                "league_sport": item.get("strSport"),
                "league_name_alternate": item.get("strLeagueAlternate") or "",
                "created_at": now,
            })

    logger.info("Fetched %d leagues from TheSportsDB.", len(leagues))
    return leagues


def fetch_league_details(
    client: RateLimitedClient,
    league_ids: list[int],
) -> list[dict]:
    """Fetch extended metadata for each league in league_ids.

    Makes one API call per league. Skips leagues where the response
    contains no 'lookup' data and logs a warning.
    Returns list of dicts suitable for upsert into api_league_details.
    """
    details: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for league_id in league_ids:
        url = league_detail_url(league_id)
        try:
            data = client.get(url)
        except httpx.HTTPError as exc:
            logger.warning("Failed to fetch details for league %s: %s", league_id, exc)
            continue

        lookup = data.get("lookup") or []
        if not lookup:
            logger.warning("No detail data for league %s.", league_id)
            continue

        for item in lookup:
            details.append({
                "league_id": item.get("idLeague"),
                "league_name": item.get("strLeague"),
                "league_sport": item.get("strSport"),
                "league_name_alternate": item.get("strLeagueAlternate"),
                "league_division": item.get("intDivision"),
                "league_cup": item.get("idCup"),
                "league_current_season": item.get("strCurrentSeason"),
                "league_formed_year": item.get("intFormedYear"),
                "league_first_event": item.get("dateFirstEvent"),
                "league_gender": item.get("strGender"),
                "league_country": item.get("strCountry"),
                "league_description_en": item.get("strDescriptionEN"),
                "league_badge": item.get("strBadge"),
                "league_trophy": item.get("strTrophy"),
                "league_complete": item.get("strComplete"),
                "created_at": now,
            })

    logger.info("Fetched details for %d leagues.", len(details))
    return details
