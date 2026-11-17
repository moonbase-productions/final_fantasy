#!/usr/bin/env python3
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


def main():
    with open("events_flat_backup.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    team_info = {}
    seasons_seen = set()
    for r in rows:
        seasons_seen.add(r["season"])
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

    league_details_fields = [
        "league_id", "league_name", "league_sport", "league_name_alternate",
        "league_division", "league_cup", "league_current_season", "league_formed_year",
        "league_first_event", "league_gender", "league_country", "league_description_en",
        "league_badge", "league_trophy", "league_complete", "created_at",
    ]
    with open("league_details.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=league_details_fields)
        w.writeheader()
        w.writerow({
            "league_id": LEAGUE_ID,
            "league_name": LEAGUE_NAME,
            "league_sport": LEAGUE_SPORT,
            "league_name_alternate": LEAGUE_NAME_ALT,
            "league_division": "",
            "league_cup": "",
            "league_current_season": "2026",
            "league_formed_year": "",
            "league_first_event": "",
            "league_gender": "Female",
            "league_country": "USA",
            "league_description_en": "",
            "league_badge": "",
            "league_trophy": "",
            "league_complete": "",
            "created_at": NOW,
        })
    print("Wrote 1 row to league_details.csv")

    season_fields = ["league_id", "league_season", "created_at", "updated_at"]
    with open("seasons.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=season_fields)
        w.writeheader()
        for season in sorted(seasons_seen):
            w.writerow({
                "league_id": LEAGUE_ID,
                "league_season": season,
                "created_at": NOW,
                "updated_at": NOW,
            })
    print(f"Wrote {len(seasons_seen)} rows to seasons.csv")

    team_details_fields = [
        "team_id", "id_espn", "id_api_football", "team_name", "team_alternate", "team_short",
        "formed_year", "sport", "league_name", "league_id",
        "league2_name", "league2_id", "league3_name", "league3_id", "league4_name", "league4_id",
        "league5_name", "league5_id", "league6_name", "league6_id", "league7_name", "league7_id",
        "division", "venue_id", "stadium", "stadium_capacity", "location", "website",
        "facebook", "twitter", "instagram", "youtube", "description_en",
        "colour1", "colour2", "colour3", "gender", "country", "badge", "logo",
        "fanart1", "fanart2", "fanart3", "fanart4", "banner", "equipment", "locked",
        "created_at", "updated_at", "lat", "lon",
        "uid", "conference",
    ]
    with open("team_details.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=team_details_fields)
        w.writeheader()
        for i, key in enumerate(ordered_keys, start=1):
            info = team_info[key]
            seo = info["seo"]
            uid = f"{LEAGUE_ID}-{i}"
            w.writerow({
                "team_id": i,
                "id_espn": "",
                "id_api_football": "",
                "team_name": info["name"],
                "team_alternate": "",
                "team_short": "",
                "formed_year": "",
                "sport": LEAGUE_SPORT,
                "league_name": LEAGUE_NAME,
                "league_id": LEAGUE_ID,
                "league2_name": "", "league2_id": "",
                "league3_name": "", "league3_id": "",
                "league4_name": "", "league4_id": "",
                "league5_name": "", "league5_id": "",
                "league6_name": "", "league6_id": "",
                "league7_name": "", "league7_id": "",
                "division": "",
                "venue_id": "",
                "stadium": "",
                "stadium_capacity": "",
                "location": "",
                "website": "",
                "facebook": "",
                "twitter": "",
                "instagram": "",
                "youtube": "",
                "description_en": "",
                "colour1": "", "colour2": "", "colour3": "",
                "gender": "Female",
                "country": "USA",
                "badge": "",
                "logo": LOGO_URL_TMPL.format(seo=seo) if seo else "",
                "fanart1": "", "fanart2": "", "fanart3": "", "fanart4": "",
                "banner": "",
                "equipment": "",
                "locked": "",
                "created_at": NOW,
                "updated_at": NOW,
                "lat": "",
                "lon": "",
                "uid": uid,
                "conference": info["conference"],
            })
    print(f"Wrote {len(ordered_keys)} rows to team_details.csv")


if __name__ == "__main__":
    main()
