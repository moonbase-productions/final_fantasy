"""tests/test_sql/test_query_inventory.py"""
from __future__ import annotations

from pathlib import Path


def test_query_file_inventory_exact() -> None:
    root = Path(__file__).resolve().parents[2]
    query_dir = root / "src" / "pipeline" / "sql" / "queries"

    expected = [
        "01_leagues_current.sql",
        "02_season_windows.sql",
        "03_web_events.sql",
        "04_events_scored.sql",
        "05_events_split.sql",
        "06_asset_last_10.sql",
        "07_events_future_elos.sql",
        "08_assets_future.sql",
        "09_assets_stats_at.sql",
        "10_forecast.sql",
        "11_assets_season_to_date.sql",
        "12_ref_elo.sql",
        "13_current_elo.sql",
        "14_wld.sql",
        "15_web_assets.sql",
        "16_web_assets_info.sql",
        "17_league_info.sql",
    ]

    found = sorted(p.name for p in query_dir.glob("*.sql"))
    assert found == expected


def test_query_files_are_non_empty() -> None:
    root = Path(__file__).resolve().parents[2]
    query_dir = root / "src" / "pipeline" / "sql" / "queries"
    for p in sorted(query_dir.glob("*.sql")):
        assert p.read_text(encoding="utf-8").strip(), f"{p.name} is empty"
