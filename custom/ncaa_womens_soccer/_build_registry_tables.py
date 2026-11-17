#!/usr/bin/env python3
"""
Recreate leagues.csv / assets.csv / events.csv for D1 Women's Soccer in the
schema of example_leagues_rows.csv / example_assets_rows.csv / example_events_rows.csv.

Reads the existing flattened events.csv (built by build_tables.py from
season_2025.csv + season_2026.csv) and reshapes it into the registry schema.

Fields the source data doesn't have (team_short, stadium, twitter, etc.) are
left empty rather than invented.
"""
import csv
import re
from datetime import datetime, timezone

LEAGUE_ID = 9000
LEAGUE_NAME = "NCAA Division I Women's Soccer"
LEAGUE_SPORT = "Soccer"
LEAGUE_NAME_ALT = "NCAA D1 Women's Soccer, NCAAW Soccer, D1 Women's Soccer"
LOGO_URL_TMPL = "https://ncaa-api.henrygd.me/logo/{seo}.svg"

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") + "+00"


def slugify(name):
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def parse_time(t):
    t = (t or "").replace(" ET", "").strip()
    if not t:
        return ""
    try:
        return datetime.strptime(t, "%I:%M %p").strftime("%H:%M:%S")
    except ValueError:
        return ""


def main():
    with open("events.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    team_info = {}
    for r in rows:
        for side in ("home", "away"):
            seo = r[f"{side}_seo"]
            name = r[f"{side}_team"]
            key = seo if seo else slugify(name)
            conf = r[f"{side}_conference"]
            if key not in team_info:
                team_info[key] = {"name": name, "conference": conf, "seo": seo}
            else:
                if conf and (not team_info[key]["conference"] or r["season"] == "2026"):
                    team_info[key]["conference"] = conf
                if name and r["season"] == "2026":
                    team_info[key]["name"] = name

    ordered_keys = sorted(team_info.keys(), key=lambda k: team_info[k]["name"])
    key_to_uid = {}
    assets = []
    for i, key in enumerate(ordered_keys, start=1):
        info = team_info[key]
        uid = f"{LEAGUE_ID}-{i}"
        key_to_uid[key] = uid
        seo = info["seo"]
        assets.append({
            "uid": uid,
            "league_id": LEAGUE_ID,
            "team_name": info["name"],
            "team_short": "",
            "team_logo": LOGO_URL_TMPL.format(seo=seo) if seo else "",
            "team_country": "USA",
            "created_at": NOW,
            "updated_at": NOW,
        })

    asset_fields = ["uid", "league_id", "team_name", "team_short", "team_logo",
                     "team_country", "created_at", "updated_at"]
    with open("assets.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=asset_fields)
        w.writeheader()
        w.writerows(assets)
    print(f"Wrote {len(assets)} rows to assets.csv")

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

    event_fields = ["event_id", "event_date", "event_time", "league_id", "league_sport",
                     "league_season", "league_round", "uid_home", "team_score_home",
                     "uid_away", "team_score_away", "event_status", "event_video",
                     "updated_at", "ncaa_url"]
    events_out = []
    for r in rows:
        home_key = r["home_seo"] if r["home_seo"] else slugify(r["home_team"])
        away_key = r["away_seo"] if r["away_seo"] else slugify(r["away_team"])
        events_out.append({
            "event_id": r["event_id"],
            "event_date": r["date"],
            "event_time": parse_time(r["start_time_et"]),
            "league_id": LEAGUE_ID,
            "league_sport": LEAGUE_SPORT,
            "league_season": r["season"],
            "league_round": "",
            "uid_home": key_to_uid[home_key],
            "team_score_home": r["home_score"],
            "uid_away": key_to_uid[away_key],
            "team_score_away": r["away_score"],
            "event_status": r["status"],
            "event_video": "",
            "updated_at": NOW,
            "ncaa_url": r["ncaa_url"],
        })

    with open("events.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=event_fields)
        w.writeheader()
        w.writerows(events_out)
    print(f"Wrote {len(events_out)} rows to events.csv")


if __name__ == "__main__":
    main()
