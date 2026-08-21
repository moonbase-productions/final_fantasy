"""Force-refresh of ALL league details, including ones already fetched.

Run: python -m pipeline.runners.fetch_league_details

full_refresh already keeps api.league_details filled in automatically —
each run fetches details for any league that doesn't have a row yet, so
new leagues are covered without extra intervention. Use THIS script only
when you need to re-fetch metadata for leagues that already have a row
(e.g. a league was renamed, or you suspect stale/incomplete data) and
want to force a complete refetch across every league in api.leagues.

This is a heavy, one-call-per-league operation (TheSportsDB has
thousands of leagues, and the API is rate-limited to 100 req/min), so it
is intentionally never run automatically — always on demand.

Requires api.leagues to already be populated (i.e. full_refresh has run
at least once) — this reads league IDs from api.leagues rather than
re-fetching the whole leagues list from TheSportsDB.
"""
from __future__ import annotations
import logging
import sys
import time
from datetime import datetime, timezone

from pipeline.config import validate_runtime_settings
from pipeline.db import get_supabase_client
from pipeline.api.client import RateLimitedClient
from pipeline.extract.leagues import fetch_league_details
from pipeline.load.upsert import batch_upsert
from pipeline.runners.shared import paginate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    start_time = time.monotonic()
    validate_runtime_settings()
    logger.info("=" * 60)
    logger.info(
        "LEAGUE DETAILS REFRESH STARTED at %s UTC",
        datetime.now(timezone.utc).isoformat(),
    )
    logger.info("=" * 60)

    supabase = get_supabase_client()

    all_leagues = paginate(supabase, "api.leagues")
    all_league_ids = sorted({
        str(row["league_id"]) for row in all_leagues if row.get("league_id")
    })
    if not all_league_ids:
        logger.warning(
            "api.leagues is empty — run full_refresh at least once before "
            "this command."
        )
        return

    logger.info(
        "Fetching details for %d leagues from api.leagues ...",
        len(all_league_ids),
    )

    with RateLimitedClient() as client:
        details = fetch_league_details(client, all_league_ids)

    batch_upsert(supabase, "api.league_details", details, "league_id")

    elapsed = time.monotonic() - start_time
    logger.info("=" * 60)
    logger.info(
        "LEAGUE DETAILS REFRESH COMPLETE in %.1fs (%d/%d leagues had details).",
        elapsed, len(details), len(all_league_ids),
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
