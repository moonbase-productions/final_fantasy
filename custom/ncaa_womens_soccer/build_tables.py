#!/usr/bin/env python3
"""
Build two flat tables from the raw per-day scrape output:

  events.csv - one row per game (all seasons), for anything schedule/results related
  teams.csv  - one row per team ("asset"): name, logo, conference, and W-L-T
               record for every season passed in, computed by aggregating that
               team's own final games (the NCAA feed doesn't reliably return a
               running "record" string, so we derive it ourselves rather than
               trust a field that's often blank).

Input: the CSVs produced by scrape_ncaa_soccer.py (one per season).

Usage:
    # explicit seasons
    python build_tables.py --season 2023=season_2023.csv --season 2024=season_2024.csv \
                            --season 2025=season_2025.csv --season 2026=season_2026.csv

    # or just point it at a directory and it'll pick up every season_*.csv
    python build_tables.py --auto
"""

import argparse
import csv
import glob
import re
from collections import defaultdict

LOGO_URL_TMPL = "https://ncaa-api.henrygd.me/logo/{seo}.svg"


def load_games(path, season_label):
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["season"] = season_label
    return rows


def build_events(all_games):
    fieldnames = [
        "event_id", "season", "date", "status", "start_time_et",
        "home_team", "home_seo", "home_conference", "home_score",
        "away_team", "away_seo", "away_conference", "away_score",
        "winner_seo", "current_period", "ncaa_url",
    ]
    events = []
    for g in all_games:
        home_seo = g["home_seo"]
        away_seo = g["away_seo"]
        winner_seo = ""
        if g["winner"]:
            # `winner` in the scrape is the short display name; match it back to a seo
            if g["winner"] == g["home_team"]:
                winner_seo = home_seo
            elif g["winner"] == g["away_team"]:
                winner_seo = away_seo
        events.append({
            "event_id": g["game_id"],
            "season": g["season"],
            "date": g["date"],
            "status": g["status"],
            "start_time_et": g["start_time_et"],
            "home_team": g["home_team"],
            "home_seo": home_seo,
            "home_conference": g["home_conference"],
            "home_score": g["home_score"],
            "away_team": g["away_team"],
            "away_seo": away_seo,
            "away_conference": g["away_conference"],
            "away_score": g["away_score"],
            "winner_seo": winner_seo,
            "current_period": g["current_period"],
            "ncaa_url": g["ncaa_url"],
        })
    return fieldnames, events


def compute_records(all_games):
    """Returns {(season, team_seo): {'w':int,'l':int,'t':int}} from final games only."""
    records = defaultdict(lambda: {"w": 0, "l": 0, "t": 0})
    for g in all_games:
        if g["status"] != "final":
            continue
        if not g["home_score"] or not g["away_score"]:
            continue
        try:
            hs, aws = int(g["home_score"]), int(g["away_score"])
        except ValueError:
            continue
        season = g["season"]
        home_key = (season, g["home_seo"])
        away_key = (season, g["away_seo"])
        if hs > aws:
            records[home_key]["w"] += 1
            records[away_key]["l"] += 1
        elif hs < aws:
            records[home_key]["l"] += 1
            records[away_key]["w"] += 1
        else:
            records[home_key]["t"] += 1
            records[away_key]["t"] += 1
    return records


def fmt_record(rec):
    if rec is None:
        return ""
    return f"{rec['w']}-{rec['l']}-{rec['t']}"


def build_teams(all_games, records, season_labels):
    # Collect team identity + most-recently-seen conference (prefer the latest season's data)
    latest_season = max(season_labels, key=lambda s: str(s))
    team_info = {}  # seo -> {name, conference}
    for g in all_games:
        for side in ("home", "away"):
            seo = g[f"{side}_seo"]
            if not seo:
                continue
            name = g[f"{side}_team"]
            conf = g[f"{side}_conference"]
            if seo not in team_info:
                team_info[seo] = {"name": name, "conference": conf}
            else:
                # prefer a non-empty conference / name, and prefer the latest season's version
                if conf and (not team_info[seo]["conference"] or g["season"] == latest_season):
                    team_info[seo]["conference"] = conf
                if name and (not team_info[seo]["name"] or g["season"] == latest_season):
                    team_info[seo]["name"] = name

    sorted_seasons = sorted(season_labels, key=lambda s: str(s))
    record_fields = [f"record_{s}" for s in sorted_seasons]

    fieldnames = [
        "team_seo", "team_name", "conference", "logo_url",
    ] + record_fields
    teams = []
    for seo, info in sorted(team_info.items(), key=lambda kv: kv[1]["name"]):
        row = {
            "team_seo": seo,
            "team_name": info["name"],
            "conference": info["conference"],
            "logo_url": LOGO_URL_TMPL.format(seo=seo),
        }
        for s in sorted_seasons:
            row[f"record_{s}"] = fmt_record(records.get((s, seo)))
        teams.append(row)
    return fieldnames, teams


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {path}")


def parse_season_arg(s):
    if "=" not in s:
        raise argparse.ArgumentTypeError("expected LABEL=PATH, e.g. 2025=season_2025.csv")
    label, path = s.split("=", 1)
    return label, path


def auto_discover_seasons():
    """Finds season_YYYY.csv files in the current directory."""
    found = []
    for path in sorted(glob.glob("season_*.csv")):
        m = re.match(r"season_(.+)\.csv$", path)
        if m:
            found.append((m.group(1), path))
    return found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", action="append", type=parse_season_arg, default=[],
                         metavar="LABEL=PATH",
                         help="a season label and its scrape CSV, e.g. --season 2025=season_2025.csv "
                              "(repeatable)")
    parser.add_argument("--auto", action="store_true",
                         help="auto-discover every season_*.csv in the current directory instead of "
                              "passing --season explicitly")
    parser.add_argument("--events-out", default="events.csv")
    parser.add_argument("--teams-out", default="teams.csv")
    args = parser.parse_args()

    seasons = args.season
    if args.auto or not seasons:
        seasons = auto_discover_seasons()
        if not seasons:
            parser.error("no season_*.csv files found; pass --season LABEL=PATH explicitly")
        print("Auto-discovered seasons:", ", ".join(f"{label} ({path})" for label, path in seasons))

    all_games = []
    for label, path in seasons:
        all_games.extend(load_games(path, label))

    season_labels = [label for label, _ in seasons]

    ev_fields, events = build_events(all_games)
    write_csv(args.events_out, ev_fields, events)

    records = compute_records(all_games)
    team_fields, teams = build_teams(all_games, records, season_labels)
    write_csv(args.teams_out, team_fields, teams)


if __name__ == "__main__":
    main()
