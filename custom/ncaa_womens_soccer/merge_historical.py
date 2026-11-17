#!/usr/bin/env python3
"""
One-time historical merge: registers league 9000 (NCAA Division I Women's
Soccer) in admin.league_registry and upserts the regenerated api-shaped
CSVs (produced by rebuild_registry.py in this directory) into the live
api.* Supabase tables.

Run this ONCE after rebuild_registry.py has produced consistent
leagues.csv / league_details.csv / seasons.csv / assets.csv /
team_details.csv / events_api.csv in this directory. After it succeeds,
`daily_update`/`full_refresh` will
pick league 9000 up automatically on every future run via
pipeline.extract.ncaa_womens_soccer (the incremental scraper), because:
  - is_active=True makes it show up in the active-league set, and
  - last_fetched_at is set to now() here, so daily_update's
    detect_new_leagues()/onboard_leagues() (which only know how to talk to
    TheSportsDB) never tries to onboard it.

Usage (from the repo root, with the pipeline package's venv active and a
real .env in place):
    python custom/ncaa_womens_soccer/merge_historical.py
"""
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make the `pipeline` package importable when run from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pipeline.db import get_supabase_client  # noqa: E402
from pipeline.load.upsert import batch_upsert  # noqa: E402

HERE = Path(__file__).parent
LEAGUE_ID = 9000
LEAGUE_NAME = "NCAA Division I Women's Soccer"
LEAGUE_SPORT = "Soccer"


def read_csv(name: str) -> list[dict]:
    path = HERE / name
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main() -> None:
    supabase = get_supabase_client()
    now_iso = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # 1. Register the league (idempotent — upsert-by-league_id).
    # ------------------------------------------------------------------
    print("--- Registering league 9000 in admin.league_registry ---")
    existing = (
        supabase.schema("admin").table("league_registry")
        .select("league_id")
        .eq("league_id", str(LEAGUE_ID))
        .execute()
        .data
    )
    registry_row = {
        "league_id": str(LEAGUE_ID),
        "league_name": LEAGUE_NAME,
        "sport_type": "standard",
        "league_sport": LEAGUE_SPORT,
        "is_whitelisted": True,
        "is_active": True,
        # Setting this now is what keeps daily_update's TheSportsDB-only
        # onboarding path from ever picking this league up.
        "last_fetched_at": now_iso,
    }
    if existing:
        supabase.schema("admin").table("league_registry").update(registry_row).eq(
            "league_id", str(LEAGUE_ID)
        ).execute()
        print("Updated existing league_registry row for 9000.")
    else:
        supabase.schema("admin").table("league_registry").insert(registry_row).execute()
        print("Inserted new league_registry row for 9000.")

    # ------------------------------------------------------------------
    # 2. Clean slate: delete any previously-merged league-9000 rows in
    #    api.assets / api.team_details / api.events before re-upserting.
    #    An earlier run of this script used a different uid/event_id scheme
    #    (uid keyed by team "seo" slug, event_id prefixed "ncaa-...") — those
    #    rows won't be overwritten by the new integer-keyed / unprefixed
    #    upsert below (different primary key = different row), so without
    #    this they'd linger as orphans alongside the correct ones.
    # ------------------------------------------------------------------
    print("--- Clearing any previously-merged league 9000 rows (assets/team_details/events) ---")
    for schema, table in (("api", "assets"), ("api", "team_details"), ("api", "events")):
        supabase.schema(schema).table(table).delete().eq("league_id", str(LEAGUE_ID)).execute()
    print("Cleared.")

    # ------------------------------------------------------------------
    # 3. Upsert reference + historical data, in FK-safe order:
    #    leagues -> league_details / seasons -> assets -> team_details -> events
    # ------------------------------------------------------------------
    leagues = read_csv("leagues.csv")
    batch_upsert(supabase, "api.leagues", leagues, "league_id")
    print(f"Upserted {len(leagues)} row(s) into api.leagues.")

    league_details = read_csv("league_details.csv")
    batch_upsert(supabase, "api.league_details", league_details, "league_id")
    print(f"Upserted {len(league_details)} row(s) into api.league_details.")

    seasons = read_csv("seasons.csv")
    batch_upsert(supabase, "api.seasons", seasons, "league_id,league_season")
    print(f"Upserted {len(seasons)} row(s) into api.seasons.")

    assets = read_csv("assets.csv")
    batch_upsert(supabase, "api.assets", assets, "uid")
    print(f"Upserted {len(assets)} row(s) into api.assets.")

    team_details = read_csv("team_details.csv")
    # team_id is BIGINT in Postgres; CSV gives it back as a string.
    for row in team_details:
        row["team_id"] = int(row["team_id"])
        for col in ("lat", "lon"):
            val = row.get(col)
            row[col] = float(val) if val not in (None, "") else None
    batch_upsert(supabase, "api.team_details", team_details, "team_id")
    print(f"Upserted {len(team_details)} row(s) into api.team_details.")

    events = read_csv("events_api.csv")
    # CSV has no types — an unplayed game's score comes back as "" (empty
    # string), which api.events.team_score_home/away (double precision)
    # rejects outright. Blank out to NULL for anything that isn't a valid
    # number.
    for row in events:
        for col in ("team_score_home", "team_score_away"):
            val = row.get(col)
            if val is None or val == "":
                row[col] = None
            else:
                try:
                    row[col] = float(val)
                except ValueError:
                    row[col] = None
    batch_upsert(supabase, "api.events", events, "event_id")
    print(f"Upserted {len(events)} row(s) into api.events.")

    print(
        "\nDone. Next: run `python -m pipeline.runners.full_refresh` (or "
        "daily_update) to rebuild derived tables, stats, and Elo with this "
        "league included."
    )


if __name__ == "__main__":
    main()
