"""Extractor for NCAA Division I Women's Basketball (league_id 9002).

TheSportsDB lists this league (5789) and its teams but has no games for it,
so like NCAA women's soccer (extract/ncaa_womens_soccer.py) it's sourced from
the NCAA scoreboard proxy (https://ncaa-api.henrygd.me), one request per day.

Design (same as women's soccer, with two differences noted):
  - uid = f"{LEAGUE_ID}-{asset_id}"; asset_id is a stable integer starting at
    TEAM_ID_START, tracked via api.assets.external_ref = NCAA's "seo" slug.
    TEAM_ID_START sits clear of soccer (9000-9543) and MASL (9600+) because
    api.team_details.team_id is one global key.
  - event_id = f"ncaawbb-{gameID}" (prefixed, unlike soccer, so it can never
    collide with a TheSportsDB or NCAA soccer event id).
  - league_season spans two years, TheSportsDB style ("2025-2026"): a game
    from July onward belongs to that year's season, earlier to the previous.
  - event_date/event_time are UTC from the feed's startTimeEpoch.
  - Trailing days are always re-fetched (no cache) to catch scores that
    finalize late; the backfill (scripts/update_ncaa_womens_basketball.py
    --since) only fetches in-season dates (SEASON_START_MMDD-SEASON_END_MMDD).
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone

import httpx

from pipeline.extract.ncaa_womens_soccer import HEADERS, STATUS_MAP, _parse_day_games

logger = logging.getLogger(__name__)

LEAGUE_ID = 9002
LEAGUE_SPORT = "Basketball"
LEAGUE_NAME = "NCAA Division I Women's Basketball"
LEAGUE_NAME_ALTERNATE = "NCAAW Basketball, Women's College Basketball, Women's March Madness"
TEAM_ID_START = 10000  # uid "9002-10000", "9002-10001", ...
EVENT_ID_PREFIX = "ncaawbb-"

PRIMARY_URL = "https://ncaa-api.henrygd.me/scoreboard/basketball-women/d1/{year}/{month:02d}/{day:02d}/all-conf"
FALLBACK_URL = "https://data.ncaa.com/casablanca/scoreboard/basketball-women/d1/{year}/{month:02d}/{day:02d}/scoreboard.json"
LOGO_URL_TMPL = "https://ncaa-api.henrygd.me/logo/{seo}.svg"

DEFAULT_DAYS_BACK = 3
DEFAULT_DAYS_FORWARD = 21
SEASON_START_MMDD = (10, 25)  # backfill only: first in-season date
SEASON_END_MMDD = (4, 15)     # backfill only: last in-season date (title game early April)


def season_label(d: date) -> str:
    start = d.year if d.month >= 7 else d.year - 1
    return f"{start}-{start + 1}"


def in_season(d: date) -> bool:
    md = (d.month, d.day)
    return md >= SEASON_START_MMDD or md <= SEASON_END_MMDD


def _fetch_day(d: date, session: httpx.Client, retries: int = 3) -> dict | None:
    for base_url in (PRIMARY_URL, FALLBACK_URL):
        url = base_url.format(year=d.year, month=d.month, day=d.day)
        for attempt in range(1, retries + 1):
            try:
                resp = session.get(url, headers=HEADERS, timeout=15)
            except httpx.HTTPError as exc:
                logger.warning("NCAA WBB fetch error for %s (attempt %d/%d): %s", d, attempt, retries, exc)
                time.sleep(1.5 * attempt)
                continue
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    break
                if "games" in data:
                    return data
                break
            if resp.status_code == 404 or "NoSuchKey" in resp.text:
                return {"games": []}
            time.sleep((2.0 if resp.status_code == 429 else 1.5) * attempt)
    logger.warning("Giving up fetching NCAA WBB scoreboard for %s", d)
    return None


def _start_times(day_json: dict) -> dict[str, int]:
    """{gameID -> startTimeEpoch} (the shared soccer parser drops it)."""
    out = {}
    for entry in day_json.get("games", []):
        g = entry.get("game", {})
        try:
            out[str(g.get("gameID"))] = int(g.get("startTimeEpoch") or 0)
        except (TypeError, ValueError):
            pass
    return out


def _resolve_team_ids(supabase, seos: set[str]) -> dict[str, int]:
    seo_to_id: dict[str, int] = {}
    max_id = TEAM_ID_START - 1
    offset, page_size = 0, 1000
    while True:
        page = (
            supabase.schema("api").table("assets")
            .select("uid,external_ref")
            .eq("league_id", str(LEAGUE_ID))
            .range(offset, offset + page_size - 1)
            .execute()
            .data
        ) or []
        for row in page:
            try:
                asset_id = int((row.get("uid") or "").rsplit("-", 1)[-1])
            except ValueError:
                continue
            max_id = max(max_id, asset_id)
            if row.get("external_ref"):
                seo_to_id[row["external_ref"]] = asset_id
        if len(page) < page_size:
            break
        offset += page_size

    next_id = max_id + 1
    for seo in sorted(seos - set(seo_to_id)):
        seo_to_id[seo] = next_id
        next_id += 1
    return seo_to_id


def fetch_ncaa_womens_basketball(
    supabase,
    start: date,
    end: date,
    only_in_season: bool = False,
    delay: float = 0.25,
) -> tuple[list[dict], list[dict], list[dict], list[str]]:
    """Fetch games for every day in [start, end].

    Returns (events, assets, team_details, seasons) shaped for api.events,
    api.assets, api.team_details and the season labels seen.
    """
    raw_games: list[dict] = []
    epochs: dict[str, int] = {}
    days = 0
    with httpx.Client() as session:
        d = start
        while d <= end:
            if not only_in_season or in_season(d):
                day_json = _fetch_day(d, session)
                if day_json:
                    raw_games.extend(_parse_day_games(day_json, d))
                    epochs.update(_start_times(day_json))
                days += 1
                time.sleep(delay)
            d += timedelta(days=1)

    # A game can show up on two days' scoreboards; keep the last one seen.
    by_id = {str(g["game_id"]): g for g in raw_games}
    games = list(by_id.values())

    names: dict[str, str] = {}
    for g in games:
        for side in ("home", "away"):
            names.setdefault(g[f"{side}_seo"], g[f"{side}_name"])

    seo_to_id = _resolve_team_ids(supabase, set(names)) if names else {}
    now_iso = datetime.now(timezone.utc).isoformat()

    def uid_for(seo: str) -> str:
        return f"{LEAGUE_ID}-{seo_to_id[seo]}"

    events: list[dict] = []
    seasons: set[str] = set()
    for g in games:
        epoch = epochs.get(str(g["game_id"]))
        if epoch:
            dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
            event_date, event_time = dt.date().isoformat(), dt.strftime("%H:%M:%S")
        else:
            event_date, event_time = g["date"].isoformat(), "12:00:00"
        season = season_label(g["date"])
        seasons.add(season)
        events.append({
            "event_id": f"{EVENT_ID_PREFIX}{g['game_id']}",
            "event_date": event_date,
            "event_time": event_time,
            "league_id": str(LEAGUE_ID),
            "league_sport": LEAGUE_SPORT,
            "league_season": season,
            "league_round": "",
            "uid_home": uid_for(g["home_seo"]),
            "team_score_home": g["home_score"],
            "uid_away": uid_for(g["away_seo"]),
            "team_score_away": g["away_score"],
            "event_status": STATUS_MAP.get(g["status"], g["status"]),
            "event_video": "",
            "updated_at": now_iso,
        })

    assets: list[dict] = []
    team_details: list[dict] = []
    for seo, name in sorted(names.items()):
        logo = LOGO_URL_TMPL.format(seo=seo)
        assets.append({
            "uid": uid_for(seo),
            "league_id": str(LEAGUE_ID),
            "team_name": name,
            "team_short": "",
            "team_logo": logo,
            "team_country": "United States",
            "external_ref": seo,
            "updated_at": now_iso,
        })
        team_details.append({
            "team_id": str(seo_to_id[seo]),
            "team_name": name,
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
        "NCAA women's basketball: %d days fetched, %d events, %d teams (%s to %s).",
        days, len(events), len(assets), start, end,
    )
    return events, assets, team_details, sorted(seasons)


def fetch_ncaa_womens_basketball_incremental(
    supabase,
    days_back: int = DEFAULT_DAYS_BACK,
    days_forward: int = DEFAULT_DAYS_FORWARD,
) -> tuple[list[dict], list[dict], list[dict], list[str]]:
    today = datetime.now(timezone.utc).date()
    return fetch_ncaa_womens_basketball(
        supabase, today - timedelta(days=days_back), today + timedelta(days=days_forward),
    )
