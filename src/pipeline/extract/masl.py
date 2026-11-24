"""Extractor for the Major Arena Soccer League (MASL, league_id 9001).

MASL isn't on TheSportsDB. Its stats live on SoccerShift (DigitalShift),
shown at https://www.maslsoccer.com/stats. The DigitalShift API behind that
page requires a site login ticket, so this module does NOT call it. Instead
it reads season snapshots: JSON exports of the site's rendered Scores page,
saved under custom/masl/scores_<season>.json (one file per season). Each
snapshot holds the DigitalShift score records for every game in that season
(game_id, date/time, teams, scores, status, logos).

Design (mirrors extract/ncaa_womens_soccer.py):
  - uid = f"{LEAGUE_ID}-{asset_id}". DigitalShift team ids change every
    season, so they can't be used as asset ids. Each team instead gets a
    stable integer (starting at TEAM_ID_START), tracked via
    api.assets.external_ref = a slug of the team name (e.g.
    "milwaukee-wave"). An existing team keeps its id; a new team gets the
    next free integer. TEAM_ID_START sits above NCAA's 9000-9543 range
    because api.team_details.team_id is a single global key.
  - event_id = f"masl-{game_id}". The prefix keeps DigitalShift's numeric
    ids from colliding with TheSportsDB event ids in the global
    api.events.event_id key.
  - league_season = the snapshot's season label, TheSportsDB style
    ("2025-2026"), so seasons sort correctly in 02_season_windows.sql.
  - event_date / event_time are UTC (from DigitalShift's datetime_tz), to
    match TheSportsDB-sourced events.
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

LEAGUE_ID = 9001
LEAGUE_SPORT = "Soccer"
LEAGUE_NAME = "Major Arena Soccer League"
LEAGUE_NAME_ALTERNATE = "MASL, Major Arena Soccer League, Arena Soccer"
LEAGUE_COUNTRY = "United States"
TEAM_ID_START = 9600  # asset/team ids for this league: 9600, 9601, ...

SNAPSHOT_DIR = Path(__file__).resolve().parents[3] / "custom" / "masl"

STATUS_MAP = {
    "final": "Match Finished",
    "scheduled": "Not Started",
    "in progress": "Live",
    "live": "Live",
    "postponed": "Postponed",
    "cancelled": "Cancelled",
    "delayed": "Postponed",
    "forfeit": "Match Finished",
}


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def snapshot_paths(snapshot_dir: Path = SNAPSHOT_DIR) -> list[Path]:
    return sorted(snapshot_dir.glob("scores_*.json"))


def load_snapshots(paths: list[Path] | None = None) -> list[tuple[str, list[dict]]]:
    """Return [(season_label, games), ...] for each snapshot file."""
    out = []
    for path in paths or snapshot_paths():
        data = json.loads(path.read_text(encoding="utf-8"))
        season = data.get("season") or path.stem.removeprefix("scores_")
        games = [g for g in data.get("games", []) if g.get("game_id")]
        out.append((season, games))
        logger.info("MASL: loaded %d games for %s from %s.", len(games), season, path.name)
    return out


def _score(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _status(game: dict) -> str:
    raw = (game.get("status") or "").strip().lower()
    if raw.startswith("final"):
        return "Match Finished"
    return STATUS_MAP.get(raw, game.get("status") or "Not Started")


def _utc_date_time(game: dict) -> tuple[str, str]:
    ts = game.get("datetime_tz")
    if ts:
        dt = datetime.fromisoformat(ts).astimezone(timezone.utc)
        return dt.date().isoformat(), dt.strftime("%H:%M:%S")
    return game["date"], f"{game.get('time') or '12:00'}:00"[:8]


def resolve_team_ids(supabase, slugs: set[str]) -> dict[str, int]:
    """{slug -> integer asset id}, reusing api.assets rows (by external_ref)
    and allocating the next free integer for unseen slugs."""
    slug_to_id: dict[str, int] = {}
    max_id = TEAM_ID_START - 1
    if supabase is not None:
        rows = (
            supabase.schema("api").table("assets")
            .select("uid,external_ref")
            .eq("league_id", str(LEAGUE_ID))
            .execute()
            .data
        ) or []
        for row in rows:
            try:
                asset_id = int((row.get("uid") or "").rsplit("-", 1)[-1])
            except ValueError:
                continue
            max_id = max(max_id, asset_id)
            if row.get("external_ref"):
                slug_to_id[row["external_ref"]] = asset_id

    next_id = max_id + 1
    for slug in sorted(slugs - set(slug_to_id)):
        slug_to_id[slug] = next_id
        logger.info("MASL: assigning new team id %d to '%s'.", next_id, slug)
        next_id += 1
    return slug_to_id


def build_masl_rows(supabase, snapshots: list[tuple[str, list[dict]]] | None = None) -> dict[str, list[dict]]:
    """Build api.*-shaped rows from MASL season snapshots.

    Returns a dict with keys: leagues, league_details, seasons, assets,
    team_details, events. All rows are upsert-safe.
    """
    snapshots = snapshots if snapshots is not None else load_snapshots()
    now_iso = datetime.now(timezone.utc).isoformat()

    # Latest info per team (name, short name, city, logo) + home venues.
    teams: dict[str, dict] = {}
    venues: dict[str, Counter] = {}
    for _season, games in snapshots:
        for g in sorted(games, key=lambda x: x.get("datetime_tz") or x.get("date") or ""):
            for side in ("home", "away"):
                name = g.get(f"{side}_team")
                if not name:
                    continue
                slug = slugify(name)
                info = teams.setdefault(slug, {})
                info["name"] = name
                info["short"] = g.get(f"{side}_team_short") or info.get("short", "")
                info["city"] = g.get(f"{side}_team_city") or info.get("city", "")
                if g.get(f"{side}_logo"):
                    info["logo"] = g[f"{side}_logo"]
            if g.get("home_team") and g.get("facility"):
                venues.setdefault(slugify(g["home_team"]), Counter())[g["facility"]] += 1

    slug_to_id = resolve_team_ids(supabase, set(teams))

    def uid_for(name: str) -> str:
        return f"{LEAGUE_ID}-{slug_to_id[slugify(name)]}"

    events: list[dict] = []
    seasons: list[dict] = []
    for season, games in snapshots:
        seasons.append({"league_id": str(LEAGUE_ID), "league_season": season, "updated_at": now_iso})
        for g in games:
            event_date, event_time = _utc_date_time(g)
            events.append({
                "event_id": f"masl-{g['game_id']}",
                "event_date": event_date,
                "event_time": event_time,
                "league_id": str(LEAGUE_ID),
                "league_sport": LEAGUE_SPORT,
                "league_season": season,
                "league_round": "Playoffs" if g.get("game_type") == "Playoffs" else "",
                "uid_home": uid_for(g["home_team"]),
                "team_score_home": _score(g.get("home_score")),
                "uid_away": uid_for(g["away_team"]),
                "team_score_away": _score(g.get("away_score")),
                "event_status": _status(g),
                "event_video": "",
                "updated_at": now_iso,
            })

    assets: list[dict] = []
    team_details: list[dict] = []
    for slug, info in sorted(teams.items()):
        logo = info.get("logo", "")
        stadium = venues[slug].most_common(1)[0][0] if slug in venues else ""
        assets.append({
            "uid": f"{LEAGUE_ID}-{slug_to_id[slug]}",
            "league_id": str(LEAGUE_ID),
            "team_name": info["name"],
            "team_short": info.get("short", ""),
            "team_logo": logo,
            "team_country": LEAGUE_COUNTRY,
            "external_ref": slug,
            "updated_at": now_iso,
        })
        team_details.append({
            "team_id": str(slug_to_id[slug]),
            "team_name": info["name"],
            "team_short": info.get("short", ""),
            "sport": LEAGUE_SPORT,
            "league_name": LEAGUE_NAME,
            "league_id": str(LEAGUE_ID),
            "gender": "Male",
            "country": LEAGUE_COUNTRY,
            "location": info.get("city", ""),
            "stadium": stadium,
            "badge": logo,
            "logo": logo,
            "locked": "unlocked",
            "updated_at": now_iso,
        })

    current_season = max((s for s, _ in snapshots), default="")
    leagues = [{
        "league_id": str(LEAGUE_ID),
        "league_name": LEAGUE_NAME,
        "league_sport": LEAGUE_SPORT,
        "league_name_alternate": LEAGUE_NAME_ALTERNATE,
    }]
    league_details = [{
        **leagues[0],
        "league_division": "1",
        "league_current_season": current_season,
        "league_gender": "Male",
        "league_country": LEAGUE_COUNTRY,
    }]

    logger.info(
        "MASL: built %d events, %d teams across %d season(s).",
        len(events), len(assets), len(seasons),
    )
    return {
        "leagues": leagues,
        "league_details": league_details,
        "seasons": seasons,
        "assets": assets,
        "team_details": team_details,
        "events": events,
    }
