"""Incremental extractor for NCAA Division I Women's Soccer (league_id 9000).

This league isn't available from TheSportsDB, so it's sourced from the free
community NCAA scoreboard proxy (https://ncaa-api.henrygd.me), the same feed
`custom/ncaa_womens_soccer/scrape_ncaa_soccer.py` uses for the one-time
historical backfill. This module re-implements just the day-scrape logic
(rather than importing across into `custom/`, which isn't part of the
installable `pipeline` package) so it can plug into daily_update/full_refresh
the same way `extract/events.py` does for TheSportsDB leagues.

Design notes:
  - uid = f"{LEAGUE_ID}-{asset_id}" where asset_id is a plain sequential
    INTEGER, matching the convention TheSportsDB-sourced leagues use (where
    asset_id is TheSportsDB's own numeric team id). NCAA has no numeric team
    id of its own, so one is assigned here and kept STABLE across runs via
    api.assets.external_ref, which stores NCAA's own stable "seo" slug
    (e.g. "duke") for each team — see _resolve_team_ids(). A team already in
    api.assets keeps its id; a genuinely new team gets the next available
    integer.
  - event_id = the raw NCAA numeric game_id, no prefix. (api.events.event_id
    is a single GLOBAL text primary key not scoped by league, so this could
    in principle collide with a TheSportsDB event id — accepted as a known,
    low-probability risk for now.)
  - The scoreboard feed only finalizes a day's scores sometime after it's
    played, and NCAA occasionally corrects a result days later. A day fetched
    once as "scheduled"/"live" would otherwise never be refetched. This
    module always force-refetches a small trailing+current window (no local
    caching) rather than the scrape script's cache-unless-`--refresh`
    behavior, which is fine for a single incremental run per day.
"""
from __future__ import annotations
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

LEAGUE_ID = 9000
LEAGUE_SPORT = "Soccer"
LEAGUE_NAME = "NCAA Division I Women's Soccer"
TEAM_ID_START = 9000  # asset ids for this league start at 9000, i.e. uid "9000-9000", "9000-9001", ...

PRIMARY_URL = "https://ncaa-api.henrygd.me/scoreboard/soccer-women/d1/{year}/{month:02d}/{day:02d}/all-conf"
FALLBACK_URL = "https://data.ncaa.com/casablanca/scoreboard/soccer-women/d1/{year}/{month:02d}/{day:02d}/scoreboard.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

STATUS_MAP = {
    "final": "Match Finished",
    "live": "Live",
    "scheduled": "Not Started",
}

LOGO_URL_TMPL = "https://ncaa-api.henrygd.me/logo/{seo}.svg"

# How far back/forward to (re-)fetch on every incremental run. Trailing days
# are re-pulled (not cached) specifically to catch scores that finalize
# after the fact and late corrections.
DEFAULT_DAYS_BACK = 3
DEFAULT_DAYS_FORWARD = 21


def _fetch_day(d: date, session: httpx.Client, retries: int = 3) -> dict | None:
    for base_url in (PRIMARY_URL, FALLBACK_URL):
        url = base_url.format(year=d.year, month=d.month, day=d.day)
        for attempt in range(1, retries + 1):
            try:
                resp = session.get(url, headers=HEADERS, timeout=15)
            except httpx.HTTPError as exc:
                logger.warning("NCAA scoreboard fetch error for %s (attempt %d/%d): %s", d, attempt, retries, exc)
                time.sleep(1.5 * attempt)
                continue

            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    break  # try fallback
                if "games" in data:
                    return data
                break  # unexpected shape - try fallback

            if resp.status_code == 404 or "NoSuchKey" in resp.text:
                return {"games": []}

            if resp.status_code == 429:
                logger.warning("NCAA scoreboard rate limited (429) for %s, backing off", d)
                time.sleep(2.0 * attempt)
                continue

            time.sleep(1.5 * attempt)

    logger.warning("Giving up fetching NCAA scoreboard for %s after retries on both endpoints", d)
    return None


def _score(val: Any) -> float | None:
    # api.events.team_score_home/away is double precision; an unplayed
    # game's score comes back as "" from the feed, which PostgREST rejects
    # outright. Must be NULL, not empty string.
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_day_games(day_json: dict, d: date) -> list[dict]:
    """Flatten one day's raw feed into candidate game dicts keyed by seo
    (not yet resolved to integer team ids)."""
    games = []
    for entry in day_json.get("games", []):
        g = entry.get("game", {})
        away = g.get("away", {})
        home = g.get("home", {})

        home_seo = home.get("names", {}).get("seo", "")
        away_seo = away.get("names", {}).get("seo", "")
        game_id = g.get("gameID", "")
        if not home_seo or not away_seo or not game_id:
            continue  # can't key a uid / event without these

        game_state = g.get("gameState", "")
        if game_state == "final":
            status = "final"
        elif game_state in ("live", "in_progress"):
            status = "live"
        else:
            status = "scheduled"

        games.append({
            "game_id": game_id,
            "date": d,
            "status": status,
            "home_seo": home_seo,
            "home_name": home.get("names", {}).get("full") or home.get("names", {}).get("short", ""),
            "home_conference": (
                (home.get("conferences") or [{}])[0].get("conferenceName")
                or (home.get("conferences") or [{}])[0].get("conferenceSeo", "")
            ),
            "home_score": _score(home.get("score")),
            "away_seo": away_seo,
            "away_name": away.get("names", {}).get("full") or away.get("names", {}).get("short", ""),
            "away_conference": (
                (away.get("conferences") or [{}])[0].get("conferenceName")
                or (away.get("conferences") or [{}])[0].get("conferenceSeo", "")
            ),
            "away_score": _score(away.get("score")),
        })
    return games


def _resolve_team_ids(supabase, seos: set[str]) -> dict[str, int]:
    """Return {seo -> integer asset id}, reusing existing api.assets rows
    (matched by external_ref) and allocating the next available integer for
    any seo not seen before."""
    seo_to_id: dict[str, int] = {}
    max_id = TEAM_ID_START - 1
    offset = 0
    page_size = 1000
    while True:
        page = (
            supabase.schema("api").table("assets")
            .select("uid,external_ref")
            .eq("league_id", str(LEAGUE_ID))
            .range(offset, offset + page_size - 1)
            .execute()
            .data
        )
        if not page:
            break
        for row in page:
            uid = row.get("uid") or ""
            try:
                asset_id = int(uid.rsplit("-", 1)[-1])
            except ValueError:
                continue
            max_id = max(max_id, asset_id)
            ext_ref = row.get("external_ref")
            if ext_ref:
                seo_to_id[ext_ref] = asset_id
        if len(page) < page_size:
            break
        offset += page_size

    next_id = max_id + 1
    for seo in sorted(seos - set(seo_to_id)):
        seo_to_id[seo] = next_id
        logger.info("NCAA women's soccer: assigning new team id %d to '%s'.", next_id, seo)
        next_id += 1

    return seo_to_id


def fetch_ncaa_womens_soccer_incremental(
    supabase,
    days_back: int = DEFAULT_DAYS_BACK,
    days_forward: int = DEFAULT_DAYS_FORWARD,
    delay: float = 0.25,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Fetch NCAA D1 women's soccer events for a trailing+forward window.

    Returns (events, assets, team_details):
      - events: api.events-shaped (matches what normalize_events()/
        batch_upsert expect)
      - assets: api.assets-shaped, one row per distinct team seen in the
        window (existing teams get a refreshed row; upsert-safe)
      - team_details: api.team_details-shaped, one row per distinct team
        seen in the window (fields NCAA's feed doesn't have are left blank)

    `supabase` is required to resolve/allocate stable integer team ids (see
    _resolve_team_ids) — the caller already has a client for upserting.
    """
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=days_back)
    end = today + timedelta(days=days_forward)

    raw_games: list[dict] = []
    with httpx.Client() as session:
        d = start
        while d <= end:
            day_json = _fetch_day(d, session)
            if day_json:
                raw_games.extend(_parse_day_games(day_json, d))
            time.sleep(delay)
            d += timedelta(days=1)

    seos: set[str] = set()
    team_name_by_seo: dict[str, str] = {}
    for game in raw_games:
        for side in ("home", "away"):
            seo = game[f"{side}_seo"]
            seos.add(seo)
            team_name_by_seo.setdefault(seo, game[f"{side}_name"])

    seo_to_id = _resolve_team_ids(supabase, seos) if seos else {}
    now_iso = datetime.now(timezone.utc).isoformat()

    def uid_for(seo: str) -> str:
        return f"{LEAGUE_ID}-{seo_to_id[seo]}"

    events: list[dict] = []
    for game in raw_games:
        events.append({
            "event_id": str(game["game_id"]),
            "event_date": game["date"].isoformat(),
            "event_time": "12:00:00",  # NCAA's startTime string isn't reliably parseable; not used downstream
            "league_id": str(LEAGUE_ID),
            "league_sport": LEAGUE_SPORT,
            "league_season": str(game["date"].year),
            "league_round": "",
            "uid_home": uid_for(game["home_seo"]),
            "team_score_home": game["home_score"],
            "uid_away": uid_for(game["away_seo"]),
            "team_score_away": game["away_score"],
            "event_status": STATUS_MAP.get(game["status"], game["status"]),
            "event_video": "",
            "updated_at": now_iso,
        })

    assets: list[dict] = []
    team_details: list[dict] = []
    for seo in seos:
        logo = LOGO_URL_TMPL.format(seo=seo)
        assets.append({
            "uid": uid_for(seo),
            "league_id": str(LEAGUE_ID),
            "team_name": team_name_by_seo[seo],
            "team_short": "",
            "team_logo": logo,
            "team_country": "United States",
            "external_ref": seo,
            "updated_at": now_iso,
        })
        team_details.append({
            "team_id": seo_to_id[seo],
            "team_name": team_name_by_seo[seo],
            "sport": LEAGUE_SPORT,
            "league_name": LEAGUE_NAME,
            "league_id": str(LEAGUE_ID),
            "division": "1",
            "gender": "Female",
            "country": "United States",
            "badge": logo,
            "logo": logo,
            "locked": "unlocked",
            "updated_at": now_iso,
        })

    logger.info(
        "NCAA women's soccer incremental fetch: %d events, %d distinct teams (%s to %s).",
        len(events), len(assets), start, end,
    )
    return events, assets, team_details
