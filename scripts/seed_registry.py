"""scripts/seed_registry.py

One-time script: discover league IDs from TheSportsDB, then populate
league_registry with the whitelist defined in WHITELIST below.

Run: python scripts/seed_registry.py

Requirements:
  - .env file with SPORTSDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
  - league_registry table already created in Supabase (run DDL from spec Ch.3 first)

The script is safe to re-run: it upserts on league_id so existing rows
are updated but not duplicated.
"""
from __future__ import annotations

import logging
import os
import sys
from difflib import get_close_matches

import httpx
from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

load_dotenv()

# ── Whitelist ─────────────────────────────────────────────────────────────────
# Tuples of (league_name_to_search, sport_type, is_active)
# league_name_to_search is matched (case-insensitive, fuzzy) against
# the strLeague / strLeagueAlternate fields in TheSportsDB.
# is_active=True means the league is included in daily updates immediately.
# is_active=False means it's whitelisted for weekly refresh only.
# is_whitelisted is always True for everything in this list.

WHITELIST: list[tuple[str, str, bool]] = [
    # Soccer — active
    ("English Premier League",           "standard", True),
    ("English Championship",             "standard", True),
    ("Mexican Primera League",           "standard", True),
    # Soccer — whitelisted
    ("American Major League Soccer",     "standard", False),
    ("Argentinian Primera Division",     "standard", False),
    ("Australian A-League",              "standard", False),
    ("Austrian Football Bundesliga",     "standard", False),
    ("Belgian Pro League",               "standard", False),
    ("Brazilian Serie A",                "standard", False),
    ("Dutch Eredivisie",                 "standard", False),
    ("French Ligue 1",                   "standard", False),
    ("German Bundesliga",                "standard", False),
    ("Italian Serie A",                  "standard", False),
    ("Spanish La Liga",                  "standard", False),
    ("English League 1",                 "standard", False),
    ("English League 2",                 "standard", False),
    ("Japanese J1 League",               "standard", False),
    ("American NWSL",                    "standard", False),
    ("Australian A-League Women",        "standard", False),
    ("Bangladesh Premier League",        "standard", False),
    # Motorsports
    ("Formula 1",                        "multi_competitor", False),
    ("Formula E",                        "multi_competitor", False),
    ("NASCAR Cup Series",                "multi_competitor", False),
    ("Formula 2",                        "multi_competitor", False),
    # Fighting
    ("UFC",                              "binary", False),
    ("Boxing",                           "binary", False),
    ("Cage Warriors",                    "binary", False),
    ("WWE",                              "binary", False),
    ("Professional Fighters League",     "binary", False),
    # Baseball
    ("Korean KBO League",                "standard", False),
    ("MLB",                              "standard", False),
    ("Nippon Professional Baseball",     "standard", False),
    ("Cuban National Series",            "standard", False),
    ("Chinese Professional Baseball",    "standard", False),
    ("NCAA Division I Baseball",         "standard", False),
    # Basketball — active
    ("NBA",                              "standard", True),
    ("Chinese CBA",                      "standard", True),
    # Basketball — whitelisted
    ("Euroleague Basketball",            "standard", False),
    ("WNBA",                             "standard", False),
    ("Mexican LNBP",                     "standard", False),
    ("Spanish Liga ACB",                 "standard", False),
    ("NCAA Division I Men's Basketball", "standard", False),
    ("NCAA Division I Women's Basketball","standard", False),
    # American Football — active
    ("NFL",                              "standard", True),
    # American Football — whitelisted
    ("CFL",                              "standard", False),
    ("NCAA Division I Football",         "standard", False),
    # Hockey — active
    ("Swedish Hockey League",            "standard", True),
    # Hockey — whitelisted
    ("Finnish Liiga",                    "standard", False),
    ("German DEL",                       "standard", False),
    ("NHL",                              "standard", False),
    ("Swiss National League A",          "standard", False),
    ("NCAA Division I Hockey",           "standard", False),
    # Rugby — active
    ("English Premiership Rugby",        "standard", True),
    # Rugby — whitelisted
    ("Australian National Rugby League", "standard", False),
    ("English Rugby League Super League","standard", False),
    ("French Top 14",                    "standard", False),
    ("Super Rugby",                      "standard", False),
    ("United Rugby Championship",        "standard", False),
    # Tennis
    ("ATP World Tour",                   "binary", False),
    ("WTA Tour",                         "binary", False),
    # Cricket
    ("Australian Big Bash League",       "standard", False),
    ("English T20 Blast",                "standard", False),
    ("Indian Premier League",            "standard", False),
    # Cycling
    ("UCI World Tour",                   "multi_competitor", False),
    # E-Sports
    ("BLAST Premier",                    "standard", False),
    ("Call of Duty League",              "standard", False),
    ("ESL Pro League",                   "standard", False),
    ("League of Legends EMEA Championship","standard", False),
    ("League of Legends Pro League",     "standard", False),
    ("League of the Americas",           "standard", False),
]


# ── API helpers ───────────────────────────────────────────────────────────────

def fetch_all_leagues(api_key: str) -> list[dict]:
    """Fetch all leagues from TheSportsDB V2."""
    url = "https://www.thesportsdb.com/api/v2/json/all/leagues"
    resp = httpx.get(url, headers={"X-API-KEY": api_key}, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()
    # The all/leagues endpoint returns a list at the top level or nested
    if isinstance(data, list):
        return data
    for key in ("leagues", "countrys", "list"):
        if key in data and data[key]:
            return data[key]
    # Fallback: flatten any nested lists
    result = []
    for v in data.values():
        if isinstance(v, list):
            result.extend(v)
    return result


def build_name_map(leagues: list[dict]) -> dict[str, dict]:
    """Build a normalised name → league dict for fuzzy matching."""
    name_map: dict[str, dict] = {}
    for league in leagues:
        primary = (league.get("strLeague") or "").strip()
        alternate = (league.get("strLeagueAlternate") or "").strip()
        if primary:
            name_map[primary.lower()] = league
        if alternate:
            name_map[alternate.lower()] = league
    return name_map


def find_league(
    search_name: str,
    name_map: dict[str, dict],
    cutoff: float = 0.75,
) -> dict | None:
    """Fuzzy-match a league name against the name map. Returns best match or None."""
    key = search_name.lower()
    # Exact match first
    if key in name_map:
        return name_map[key]
    # Fuzzy match
    matches = get_close_matches(key, name_map.keys(), n=1, cutoff=cutoff)
    if matches:
        logger.info("  '%s' → fuzzy matched to '%s'", search_name, matches[0])
        return name_map[matches[0]]
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    api_key = os.environ.get("SPORTSDB_API_KEY")
    if not api_key:
        logger.error("SPORTSDB_API_KEY not set in environment.")
        sys.exit(1)

    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

    logger.info("Fetching all leagues from TheSportsDB ...")
    all_leagues = fetch_all_leagues(api_key)
    logger.info("  Retrieved %d leagues.", len(all_leagues))

    name_map = build_name_map(all_leagues)

    records: list[dict] = []
    not_found: list[str] = []

    for search_name, sport_type, is_active in WHITELIST:
        league = find_league(search_name, name_map)
        if league is None:
            logger.warning("  NOT FOUND: '%s'", search_name)
            not_found.append(search_name)
            continue

        league_id   = league.get("idLeague") or league.get("league_id")
        league_name = league.get("strLeague") or search_name
        league_sport= league.get("strSport") or ""

        records.append({
            "league_id":     int(league_id),
            "league_name":   league_name,
            "league_sport":  league_sport,
            "sport_type":    sport_type,
            "is_whitelisted": True,
            "is_active":     is_active,
            "display_name":  None,
            "notes":         f"seeded from whitelist: {search_name}",
        })
        logger.info(
            "  ✓ '%s' → id=%s, sport=%s, active=%s",
            league_name, league_id, league_sport, is_active,
        )

    if not_found:
        logger.warning(
            "\n%d leagues NOT FOUND in TheSportsDB:\n  %s\n"
            "Add them manually via the Admin UI or re-run after correcting the name.",
            len(not_found), "\n  ".join(not_found),
        )

    if not records:
        logger.error("No leagues matched. Check SPORTSDB_API_KEY and network access.")
        sys.exit(1)

    # Deduplicate by league_id — fuzzy matching can produce the same id twice
    seen: set[str] = set()
    unique_records: list[dict] = []
    for r in records:
        if r["league_id"] not in seen:
            seen.add(r["league_id"])
            unique_records.append(r)
    records = unique_records

    logger.info("Upserting %d records into league_registry ...", len(records))
    supabase = create_client(supabase_url, supabase_key)

    # Chunk upserts (100 at a time)
    for i in range(0, len(records), 100):
        chunk = records[i: i + 100]
        supabase.schema("admin").table("league_registry").upsert(
            chunk, on_conflict="league_id"
        ).execute()

    logger.info("Done. %d leagues seeded.", len(records))
    if not_found:
        logger.info(
            "Action required: manually add %d missing leagues in Admin UI.", len(not_found)
        )


if __name__ == "__main__":
    main()
