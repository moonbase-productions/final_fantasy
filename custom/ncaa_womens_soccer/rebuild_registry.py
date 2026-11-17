#!/usr/bin/env python3
"""
Rebuild leagues.csv / league_details.csv / seasons.csv / assets.csv /
team_details.csv / events_api.csv for NCAA Division I Women's Soccer
(league_id 9000) in ONE consistent pass.

This replaces _build_registry_tables.py + _build_detail_tables.py, which read
from each other's outputs in a fragile order (events.csv got reshaped in
place, then a later re-run of build_tables.py silently clobbered it back to
raw/flat format, and assets.csv/seasons.csv were left stale relative to the
full 2023-2026 dataset). Running THIS script end-to-end regenerates every
api-shaped table from the two authoritative flat sources in a single pass,
so they can never drift apart from each other again:

    - events.csv   (raw/flat, built by build_tables.py from season_*.csv;
                     one row per game, ALL seasons)
    - teams.csv    (raw/flat, built by build_tables.py from season_*.csv;
                     one row per team with per-season W-L-T records)

uid scheme: uid = "{league_id}-{asset_id}" where asset_id is a plain
sequential INTEGER (matching the convention used for TheSportsDB-sourced
leagues, where asset_id is TheSportsDB's own numeric team id). Since NCAA
has no numeric team id of its own, one is assigned here — and to keep it
STABLE across re-runs (so a re-generation never silently changes a team's
uid out from under already-loaded events/stats), assignments are persisted
in team_id_map.csv (team_seo,team_id) in this directory: existing teams keep
their id, only genuinely new team_seo values get the next available integer.

event_id: just the raw NCAA numeric game_id, no prefix. (api.events.event_id
is a single GLOBAL text primary key not scoped by league, so a NCAA game_id
could in principle collide with a TheSportsDB event id — accepted as a
known, low-probability risk for now rather than prefixing.)

IMPORTANT: this script reads the *raw/flat* events.csv/teams.csv (the
build_tables.py output) and writes the *api-shaped* tables to NEW files
(leagues.csv, league_details.csv, seasons.csv, assets.csv, team_details.csv,
events_api.csv) so the raw flat files are never overwritten. Re-run
build_tables.py first if you need to refresh the raw flat files from newer
season_*.csv scrapes, then run this script.

Usage:
    python rebuild_registry.py
"""
import csv
from datetime import datetime, timezone

LEAGUE_ID = 9000
LEAGUE_NAME = "NCAA Division I Women's Soccer"
LEAGUE_SPORT = "Soccer"
LEAGUE_NAME_ALT = "NCAA D1 Women's Soccer, NCAAW Soccer, D1 Women's Soccer"
LOGO_URL_TMPL = "https://ncaa-api.henrygd.me/logo/{seo}.svg"

TEAM_ID_MAP_FILE = "team_id_map.csv"

# Raw NCAA status strings -> the style of value TheSportsDB-sourced leagues use.
STATUS_MAP = {
    "final": "Match Finished",
    "live": "Live",
    "scheduled": "Not Started",
}

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") + "+00"


def load_team_id_map() -> dict[str, int]:
    try:
        with open(TEAM_ID_MAP_FILE, newline="", encoding="utf-8") as f:
            return {r["team_seo"]: int(r["team_id"]) for r in csv.DictReader(f)}
    except FileNotFoundError:
        return {}


def save_team_id_map(mapping: dict[str, int]) -> None:
    with open(TEAM_ID_MAP_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["team_seo", "team_id"])
        w.writeheader()
        for seo, tid in sorted(mapping.items(), key=lambda kv: kv[1]):
            w.writerow({"team_seo": seo, "team_id": tid})


TEAM_ID_START = 9000  # asset ids for this league start at 9000, i.e. uid "9000-9000", "9000-9001", ...


def assign_ids(all_seos: set[str], existing_map: dict[str, int]) -> dict[str, int]:
    """Keep every existing team_seo -> team_id assignment; give new ones the
    next available integer (starting at TEAM_ID_START), in deterministic
    (sorted) order."""
    mapping = dict(existing_map)
    next_id = (max(mapping.values()) + 1) if mapping else TEAM_ID_START
    for seo in sorted(all_seos - set(mapping)):
        mapping[seo] = next_id
        next_id += 1
    return mapping


def event_id_for(raw_event_id: str) -> str:
    return str(raw_event_id)


def parse_time(t: str) -> str:
    t = (t or "").replace(" ET", "").strip()
    if not t:
        return "12:00:00"
    for fmt in ("%I:%M %p",):
        try:
            return datetime.strptime(t, fmt).strftime("%H:%M:%S")
        except ValueError:
            pass
    return "12:00:00"


def main():
    with open("teams.csv", newline="", encoding="utf-8") as f:
        teams_raw = list(csv.DictReader(f))
    with open("events.csv", newline="", encoding="utf-8") as f:
        events_raw = list(csv.DictReader(f))

    if not teams_raw:
        raise SystemExit("teams.csv is empty or missing - run build_tables.py first")
    if not events_raw:
        raise SystemExit("events.csv is empty or missing - run build_tables.py first")

    # --- Assign stable integer ids ---
    all_seos = {t["team_seo"] for t in teams_raw if t["team_seo"]}
    for r in events_raw:
        for side in ("home", "away"):
            seo = r[f"{side}_seo"]
            if seo:
                all_seos.add(seo)

    existing_map = load_team_id_map()
    team_id_map = assign_ids(all_seos, existing_map)
    save_team_id_map(team_id_map)
    new_count = len(team_id_map) - len(existing_map)
    print(f"team_id_map.csv: {len(team_id_map)} teams total ({new_count} newly assigned).")

    def uid_for(seo: str) -> str:
        return f"{LEAGUE_ID}-{team_id_map[seo]}"

    # --- assets.csv: one row per team in the authoritative teams.csv ---
    asset_fields = ["uid", "league_id", "team_name", "team_short", "team_logo",
                     "team_country", "external_ref", "created_at", "updated_at"]
    assets = []
    team_name_by_seo: dict[str, str] = {}
    known_seos = set()
    for t in teams_raw:
        seo = t["team_seo"]
        known_seos.add(seo)
        team_name_by_seo[seo] = t["team_name"]
        assets.append({
            "uid": uid_for(seo),
            "league_id": LEAGUE_ID,
            "team_name": t["team_name"],
            "team_short": "",
            "team_logo": t["logo_url"] or (LOGO_URL_TMPL.format(seo=seo) if seo else ""),
            "team_country": "United States",
            "external_ref": seo,
            "created_at": NOW,
            "updated_at": NOW,
        })

    # Guard: every team referenced by an event must exist in assets, or the
    # api.events uid_home/uid_away references would dangle. Any event side
    # whose seo is missing from teams.csv (e.g. blank seo in the raw scrape)
    # gets added here as a fallback so nothing is silently dropped.
    extra_seos = set()
    for r in events_raw:
        for side in ("home", "away"):
            seo = r[f"{side}_seo"]
            if seo and seo not in known_seos:
                extra_seos.add(seo)
    for seo in sorted(extra_seos):
        name = next(
            (r[f"{side}_team"] for r in events_raw for side in ("home", "away")
             if r[f"{side}_seo"] == seo),
            seo,
        )
        team_name_by_seo[seo] = name
        assets.append({
            "uid": uid_for(seo),
            "league_id": LEAGUE_ID,
            "team_name": name,
            "team_short": "",
            "team_logo": LOGO_URL_TMPL.format(seo=seo),
            "team_country": "United States",
            "external_ref": seo,
            "created_at": NOW,
            "updated_at": NOW,
        })
        known_seos.add(seo)

    with open("assets.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=asset_fields)
        w.writeheader()
        w.writerows(assets)
    print(f"Wrote {len(assets)} rows to assets.csv"
          f" ({len(extra_seos)} recovered from events.csv that weren't in teams.csv)")

    # --- team_details.csv: extended per-team profile, best-effort. Fields
    # NCAA's feed doesn't have (venue, colors, socials, description, ...)
    # are left blank rather than invented. ---
    detail_fields = [
        "team_id", "id_espn", "id_api_football", "team_name", "team_alternate",
        "team_short", "formed_year", "sport", "league_name", "league_id",
        "league2_name", "league2_id", "league3_name", "league3_id",
        "league4_name", "league4_id", "league5_name", "league5_id",
        "league6_name", "league6_id", "league7_name", "league7_id",
        "division", "venue_id", "stadium", "stadium_capacity", "location",
        "website", "facebook", "twitter", "instagram", "youtube",
        "description_en", "colour1", "colour2", "colour3", "gender",
        "country", "badge", "logo", "fanart1", "fanart2", "fanart3",
        "fanart4", "banner", "equipment", "locked", "created_at",
        "updated_at", "lat", "lon",
    ]
    team_details = []
    for seo, tid in sorted(team_id_map.items(), key=lambda kv: kv[1]):
        if seo not in team_name_by_seo:
            continue  # id was assigned but the team doesn't appear in this run's data
        logo = LOGO_URL_TMPL.format(seo=seo)
        row = {k: "" for k in detail_fields}
        row.update({
            "team_id": tid,
            "team_name": team_name_by_seo[seo],
            "sport": LEAGUE_SPORT,
            "league_name": LEAGUE_NAME,
            "league_id": LEAGUE_ID,
            "division": "1",
            "gender": "Female",
            "country": "United States",
            "badge": logo,
            "logo": logo,
            "locked": "unlocked",
            "created_at": NOW,
            "updated_at": NOW,
        })
        team_details.append(row)

    with open("team_details.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=detail_fields)
        w.writeheader()
        w.writerows(team_details)
    print(f"Wrote {len(team_details)} rows to team_details.csv")

    # --- events_api.csv: api-shaped events, written to a NEW file so the
    # raw/flat events.csv (build_tables.py's authoritative output) is never
    # clobbered again. ---
    event_fields = ["event_id", "event_date", "event_time", "league_id", "league_sport",
                     "league_season", "league_round", "uid_home", "team_score_home",
                     "uid_away", "team_score_away", "event_status", "event_video",
                     "updated_at"]
    events_out = []
    seasons_seen = set()
    skipped = 0
    for r in events_raw:
        home_seo, away_seo = r["home_seo"], r["away_seo"]
        if not home_seo or not away_seo:
            skipped += 1
            continue
        seasons_seen.add(r["season"])
        events_out.append({
            "event_id": event_id_for(r["event_id"]),
            "event_date": r["date"],
            "event_time": parse_time(r["start_time_et"]),
            "league_id": LEAGUE_ID,
            "league_sport": LEAGUE_SPORT,
            "league_season": r["season"],
            "league_round": "",
            "uid_home": uid_for(home_seo),
            "team_score_home": r["home_score"],
            "uid_away": uid_for(away_seo),
            "team_score_away": r["away_score"],
            "event_status": STATUS_MAP.get(r["status"], r["status"]),
            "event_video": "",
            "updated_at": NOW,
        })

    with open("events_api.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=event_fields)
        w.writeheader()
        w.writerows(events_out)
    print(f"Wrote {len(events_out)} rows to events_api.csv ({skipped} rows skipped: missing seo)")

    # --- seasons.csv: every season actually present in events_api.csv ---
    season_fields = ["league_id", "league_season", "created_at", "updated_at"]
    with open("seasons.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=season_fields)
        w.writeheader()
        for s in sorted(seasons_seen):
            w.writerow({"league_id": LEAGUE_ID, "league_season": s,
                        "created_at": NOW, "updated_at": NOW})
    print(f"Wrote {len(seasons_seen)} rows to seasons.csv: {sorted(seasons_seen)}")

    # --- leagues.csv ---
    league_fields = ["league_id", "league_name", "league_sport", "league_name_alternate", "created_at"]
    with open("leagues.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=league_fields)
        w.writeheader()
        w.writerow({
            "league_id": LEAGUE_ID,
            "league_name": LEAGUE_NAME,
            "league_sport": LEAGUE_SPORT,
            "league_name_alternate": LEAGUE_NAME_ALT,
            "created_at": NOW,
        })
    print("Wrote 1 row to leagues.csv")

    # --- league_details.csv ---
    latest_season = max(seasons_seen) if seasons_seen else ""
    league_detail_fields = ["league_id", "league_name", "league_sport", "league_name_alternate",
                             "league_division", "league_cup", "league_current_season",
                             "league_formed_year", "league_first_event", "league_gender",
                             "league_country", "league_description_en", "league_badge",
                             "league_trophy", "league_complete", "created_at"]
    with open("league_details.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=league_detail_fields)
        w.writeheader()
        w.writerow({
            "league_id": LEAGUE_ID,
            "league_name": LEAGUE_NAME,
            "league_sport": LEAGUE_SPORT,
            "league_name_alternate": LEAGUE_NAME_ALT,
            "league_division": "1",
            "league_cup": "",
            "league_current_season": latest_season,
            "league_formed_year": "",
            "league_first_event": "",
            "league_gender": "Female",
            "league_country": "United States",
            "league_description_en": "",
            "league_badge": "",
            "league_trophy": "",
            "league_complete": "",
            "created_at": NOW,
        })
    print("Wrote 1 row to league_details.csv")

    print("\nDone. events_api.csv is the api-shaped events table to merge into api.events "
          "(the raw events.csv was left untouched).")


if __name__ == "__main__":
    main()
