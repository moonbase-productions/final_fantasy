"""tests/test_runners/test_sql_order.py"""
from __future__ import annotations

from pipeline.runners.full_refresh import DERIVED_SQL_FILES as FULL_SQL_FILES
from pipeline.runners.daily_update import DERIVED_SQL_FILES as DAILY_SQL_FILES


EXPECTED_SQL_FILES = [
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


def test_runner_sql_file_inventory_matches_expected() -> None:
    assert FULL_SQL_FILES == EXPECTED_SQL_FILES
    assert DAILY_SQL_FILES == EXPECTED_SQL_FILES


def test_runner_derived_tail_starts_after_event_tables() -> None:
    # Event window + split tables are handled before the tail loop.
    assert FULL_SQL_FILES[5] == "06_asset_last_10.sql"
    assert DAILY_SQL_FILES[5] == "06_asset_last_10.sql"


def test_runner_sql_file_order_is_unique() -> None:
    assert len(FULL_SQL_FILES) == len(set(FULL_SQL_FILES))
    assert len(DAILY_SQL_FILES) == len(set(DAILY_SQL_FILES))
