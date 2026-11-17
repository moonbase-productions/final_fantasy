#!/usr/bin/env python3
"""
NCAA D1 Women's Soccer scraper.

NCAA's public scoreboard pages (https://www.ncaa.com/scoreboard/soccer-women/d1/...)
are backed by a per-day JSON feed. There's no bulk/range endpoint, so we have to
hit it once per calendar day and stitch the results together.

As of the 2026 season, NCAA migrated their scoreboard backend from the old
data.ncaa.com REST/S3 feed to a GraphQL API (sdataprod.ncaa.com) with persisted
query hashes. Rather than reverse-engineer/maintain that, this script uses the
free, actively-maintained proxy at https://ncaa-api.henrygd.me (source:
https://github.com/henrygd/ncaa-api), which mirrors the exact same URL structure
NCAA's own site uses and returns the same JSON shape the old feed did. It falls
back to the legacy data.ncaa.com feed for older/archived seasons where that still
works. Note the public proxy is rate-limited to 5 req/s per IP - if you're
scraping a lot, consider self-hosting it via the docker-compose in that repo and
pointing PRIMARY_URL below at your own instance.

Usage:
    python scrape_ncaa_soccer.py --start 2026-08-12 --end 2026-11-01 --out results.csv

    # Just today + upcoming week
    python scrape_ncaa_soccer.py --start 2026-08-20 --end 2026-08-27

Notes:
    - Days with no games return an empty/error response. That's expected/normal
      and is just skipped.
    - Raw JSON for each day is cached in ./cache/ so re-running the script (e.g. to
      pick up new results for past "scheduled" games) doesn't re-hit days you
      already have, unless you pass --refresh.
    - Output includes both finished games ("results") and future/live games
      ("upcoming"), distinguished by the `status` column.
"""

import argparse
import csv
import json
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

# Primary: actively maintained proxy that mirrors NCAA's current backend.
PRIMARY_URL = "https://ncaa-api.henrygd.me/scoreboard/soccer-women/d1/{year}/{month:02d}/{day:02d}/all-conf"
# Fallback: legacy feed, still serves older/archived seasons (e.g. 2025 and earlier).
FALLBACK_URL = "https://data.ncaa.com/casablanca/scoreboard/soccer-women/d1/{year}/{month:02d}/{day:02d}/scoreboard.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

CACHE_DIR = Path(__file__).parent / "cache"


def daterange(start: date, end: date):
    days = (end - start).days
    for i in range(days + 1):
        yield start + timedelta(days=i)


def fetch_day(d: date, session: requests.Session, refresh: bool = False, retries: int = 3):
    """Fetch (and cache) the raw scoreboard JSON for a single day.
    Returns the parsed dict, or None if there were no games that day.
    """
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"{d.isoformat()}.json"

    if cache_file.exists() and not refresh:
        with open(cache_file, "r") as f:
            return json.load(f)

    for base_url in (PRIMARY_URL, FALLBACK_URL):
        url = base_url.format(year=d.year, month=d.month, day=d.day)

        for attempt in range(1, retries + 1):
            try:
                resp = session.get(url, headers=HEADERS, timeout=15)
            except requests.RequestException as e:
                print(f"  [{d}] request error (attempt {attempt}/{retries}): {e}", file=sys.stderr)
                time.sleep(1.5 * attempt)
                continue

            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    print(f"  [{d}] non-JSON response, skipping", file=sys.stderr)
                    break  # try fallback
                if "games" in data:
                    with open(cache_file, "w") as f:
                        json.dump(data, f)
                    return data
                break  # unexpected shape - try fallback

            if resp.status_code == 404 or "NoSuchKey" in resp.text:
                # No games that day - cache an empty marker so we don't refetch it
                with open(cache_file, "w") as f:
                    json.dump({"games": []}, f)
                return {"games": []}

            if resp.status_code == 429:
                # Rate limited - back off harder
                print(f"  [{d}] rate limited (429), backing off", file=sys.stderr)
                time.sleep(2.0 * attempt)
                continue

            # Other error codes - back off and retry
            print(f"  [{d}] HTTP {resp.status_code} (attempt {attempt}/{retries})", file=sys.stderr)
            time.sleep(1.5 * attempt)

    print(f"  [{d}] giving up after {retries} attempts on both endpoints", file=sys.stderr)
    return None


def normalize_games(day_json: dict, d: date):
    """Flatten NCAA's nested game structure into simple row dicts."""
    rows = []
    for entry in day_json.get("games", []):
        g = entry.get("game", {})
        away = g.get("away", {})
        home = g.get("home", {})

        game_state = g.get("gameState", "")  # "final", "live", "pre"
        if game_state == "final":
            status = "final"
        elif game_state in ("live", "in_progress"):
            status = "live"
        else:
            status = "scheduled"

        rows.append({
            "date": d.isoformat(),
            "status": status,
            "start_time_et": g.get("startTime", ""),
            "away_team": away.get("names", {}).get("full") or away.get("names", {}).get("short", ""),
            "away_seo": away.get("names", {}).get("seo", ""),
            "away_conference": (
                (away.get("conferences") or [{}])[0].get("conferenceName")
                or (away.get("conferences") or [{}])[0].get("conferenceSeo", "")
            ),
            "away_record": away.get("description", ""),
            "away_score": away.get("score", ""),
            "home_team": home.get("names", {}).get("full") or home.get("names", {}).get("short", ""),
            "home_seo": home.get("names", {}).get("seo", ""),
            "home_conference": (
                (home.get("conferences") or [{}])[0].get("conferenceName")
                or (home.get("conferences") or [{}])[0].get("conferenceSeo", "")
            ),
            "home_record": home.get("description", ""),
            "home_score": home.get("score", ""),
            "winner": (
                home.get("names", {}).get("short", "") if home.get("winner")
                else away.get("names", {}).get("short", "") if away.get("winner")
                else ""
            ),
            "current_period": g.get("currentPeriod", ""),
            "game_id": g.get("gameID", ""),
            "ncaa_url": ("https://www.ncaa.com" + g["url"]) if g.get("url") else "",
        })
    return rows


def scrape(start: date, end: date, refresh: bool = False, delay: float = 0.5):
    session = requests.Session()
    all_rows = []

    for d in daterange(start, end):
        day_json = fetch_day(d, session, refresh=refresh)
        if day_json is None:
            continue
        rows = normalize_games(day_json, d)
        if rows:
            print(f"  [{d}] {len(rows)} game(s)")
        all_rows.extend(rows)
        time.sleep(delay)  # be polite

    return all_rows


def write_csv(rows, out_path: Path):
    if not rows:
        print("No games found in the given date range.")
        return
    fieldnames = list(rows[0].keys())
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {out_path}")


def write_json(rows, out_path: Path):
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
    print(f"Wrote {len(rows)} rows to {out_path}")


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser(description="Scrape NCAA D1 women's soccer scoreboard by day.")
    parser.add_argument("--start", type=parse_date, required=True, help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", type=parse_date, required=True, help="End date, YYYY-MM-DD (inclusive)")
    parser.add_argument("--out", default="ncaa_womens_soccer.csv", help="Output file path (.csv or .json)")
    parser.add_argument("--refresh", action="store_true", help="Ignore cache and re-download every day")
    parser.add_argument("--delay", type=float, default=0.25, help="Seconds to sleep between requests (public proxy is capped at 5 req/s per IP)")
    args = parser.parse_args()

    if args.end < args.start:
        parser.error("--end must be on or after --start")

    print(f"Scraping D1 women's soccer scoreboard from {args.start} to {args.end}...")
    rows = scrape(args.start, args.end, refresh=args.refresh, delay=args.delay)

    out_path = Path(args.out)
    if out_path.suffix.lower() == ".json":
        write_json(rows, out_path)
    else:
        write_csv(rows, out_path)

    finals = sum(1 for r in rows if r["status"] == "final")
    upcoming = sum(1 for r in rows if r["status"] == "scheduled")
    live = sum(1 for r in rows if r["status"] == "live")
    print(f"Summary: {finals} final, {live} live, {upcoming} upcoming/scheduled.")


if __name__ == "__main__":
    main()
