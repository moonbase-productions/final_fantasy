from __future__ import annotations
import logging
from datetime import datetime, timezone

import httpx

from pipeline.api.client import RateLimitedClient
from pipeline.api.endpoints import seasons_url

logger = logging.getLogger(__name__)


def fetch_seasons(
    client: RateLimitedClient,
    league_ids: list[int],
) -> list[dict]:
    """Fetch all seasons for each league in league_ids.

    Makes one API call per league. Returns list of dicts suitable
    for upsert into api_seasons.
    """
    seasons: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for league_id in league_ids:
        url = seasons_url(league_id)
        try:
            data = client.get(url)
        except httpx.HTTPError as exc:
            logger.warning("Failed seasons for league %s: %s", league_id, exc)
            continue

        season_list = data.get("list") or []
        for item in season_list:
            season_str = item.get("strSeason")
            if not season_str:
                continue
            seasons.append({
                "league_id": str(league_id),
                "league_season": season_str,
                "updated_at": now,
            })

    logger.info("Fetched %d season records.", len(seasons))
    return seasons
