"""scripts/discover_leagues.py

Browse all leagues available from TheSportsDB API.
Standalone script — no Supabase dependency required.

Run: python scripts/discover_leagues.py
     python scripts/discover_leagues.py --sport Soccer
     python scripts/discover_leagues.py --search "Premier"

Requirements:
  - .env file with SPORTSDB_API_KEY
"""
from __future__ import annotations

import argparse
import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://www.thesportsdb.com/api/v2/json"


def fetch_all_leagues(api_key: str) -> list[dict]:
    """Fetch all leagues from TheSportsDB V2."""
    url = f"{API_BASE}/all/leagues"
    resp = httpx.get(url, headers={"X-API-KEY": api_key}, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()
    # Flatten: response may nest leagues under various keys
    if isinstance(data, list):
        return data
    result = []
    for v in data.values():
        if isinstance(v, list):
            result.extend(v)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Browse all leagues available from TheSportsDB API."
    )
    parser.add_argument(
        "--sport", type=str, default=None,
        help="Filter by sport name (case-insensitive, e.g. Soccer, Basketball, Motorsport)",
    )
    parser.add_argument(
        "--search", type=str, default=None,
        help="Search league names (case-insensitive substring match)",
    )
    parser.add_argument(
        "--format", choices=["table", "csv"], default="table",
        help="Output format (default: table)",
    )
    args = parser.parse_args()

    api_key = os.environ.get("SPORTSDB_API_KEY")
    if not api_key:
        print("ERROR: SPORTSDB_API_KEY not set in environment.", file=sys.stderr)
        sys.exit(1)

    print("Fetching all leagues from TheSportsDB ...", file=sys.stderr)
    leagues = fetch_all_leagues(api_key)
    print(f"Retrieved {len(leagues)} leagues.", file=sys.stderr)

    # Extract and clean
    rows = []
    for league in leagues:
        lid = league.get("idLeague") or ""
        name = league.get("strLeague") or ""
        sport = league.get("strSport") or ""
        alt = league.get("strLeagueAlternate") or ""
        country = league.get("strCountry") or ""
        rows.append({
            "id": str(lid),
            "name": name,
            "sport": sport,
            "alt_name": alt,
            "country": country,
        })

    # Apply filters
    if args.sport:
        sport_lower = args.sport.lower()
        rows = [r for r in rows if sport_lower in r["sport"].lower()]

    if args.search:
        search_lower = args.search.lower()
        rows = [r for r in rows if (
            search_lower in r["name"].lower()
            or search_lower in r["alt_name"].lower()
        )]

    # Sort by sport, then name
    rows.sort(key=lambda r: (r["sport"], r["name"]))

    if not rows:
        print("No leagues found matching filters.", file=sys.stderr)
        sys.exit(0)

    if args.format == "csv":
        print("id,name,sport,alt_name,country")
        for r in rows:
            # Escape commas in fields
            print(f"{r['id']},{r['name']},{r['sport']},{r['alt_name']},{r['country']}")
    else:
        # Table format
        # Determine column widths
        w_id = max(len("ID"), max(len(r["id"]) for r in rows))
        w_name = max(len("League Name"), min(45, max(len(r["name"]) for r in rows)))
        w_sport = max(len("Sport"), min(20, max(len(r["sport"]) for r in rows)))
        w_country = max(len("Country"), min(20, max(len(r["country"]) for r in rows)))

        header = (
            f"{'ID':<{w_id}}  "
            f"{'League Name':<{w_name}}  "
            f"{'Sport':<{w_sport}}  "
            f"{'Country':<{w_country}}"
        )
        print(header)
        print("-" * len(header))

        current_sport = None
        for r in rows:
            if r["sport"] != current_sport:
                if current_sport is not None:
                    print()  # Blank line between sports
                current_sport = r["sport"]
            name = r["name"][:w_name]
            sport = r["sport"][:w_sport]
            country = r["country"][:w_country]
            print(
                f"{r['id']:<{w_id}}  "
                f"{name:<{w_name}}  "
                f"{sport:<{w_sport}}  "
                f"{country:<{w_country}}"
            )

        print(f"\n{len(rows)} leagues found.", file=sys.stderr)


if __name__ == "__main__":
    main()
