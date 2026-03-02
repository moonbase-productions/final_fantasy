"""tests/conftest.py — shared pytest fixtures."""
from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture()
def minimal_events() -> pd.DataFrame:
    """Five scored events for two teams in one league."""
    return pd.DataFrame([
        {"event_id": "1", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-01", "league_sport": "Soccer",
         "uid_home": "100-1", "uid_away": "100-2",
         "team_score_home": 2.0, "team_score_away": 1.0,
         "event_result": "home", "game_order": 1},
        {"event_id": "2", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-08", "league_sport": "Soccer",
         "uid_home": "100-2", "uid_away": "100-1",
         "team_score_home": 0.0, "team_score_away": 0.0,
         "event_result": "draw", "game_order": 2},
        {"event_id": "3", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-15", "league_sport": "Soccer",
         "uid_home": "100-1", "uid_away": "100-2",
         "team_score_home": 1.0, "team_score_away": 3.0,
         "event_result": "away", "game_order": 3},
        {"event_id": "4", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-22", "league_sport": "Soccer",
         "uid_home": "100-2", "uid_away": "100-1",
         "team_score_home": 2.0, "team_score_away": 2.0,
         "event_result": "draw", "game_order": 4},
        {"event_id": "5", "league_id": "100", "league_season": "2024",
         "event_date": "2024-01-29", "league_sport": "Soccer",
         "uid_home": "100-1", "uid_away": "100-2",
         "team_score_home": 3.0, "team_score_away": 0.0,
         "event_result": "home", "game_order": 5},
    ])


@pytest.fixture()
def binary_events() -> pd.DataFrame:
    """Three UFC bouts (binary sport: scores are 1/0)."""
    return pd.DataFrame([
        {"event_id": "b1", "league_id": "200", "league_season": "2024",
         "event_date": "2024-02-01", "league_sport": "Fighting",
         "uid_home": "200-10", "uid_away": "200-11",
         "team_score_home": 1.0, "team_score_away": 0.0,
         "event_result": "home", "game_order": 1},
        {"event_id": "b2", "league_id": "200", "league_season": "2024",
         "event_date": "2024-02-08", "league_sport": "Fighting",
         "uid_home": "200-11", "uid_away": "200-12",
         "team_score_home": 1.0, "team_score_away": 0.0,
         "event_result": "home", "game_order": 2},
        {"event_id": "b3", "league_id": "200", "league_season": "2024",
         "event_date": "2024-02-15", "league_sport": "Fighting",
         "uid_home": "200-10", "uid_away": "200-12",
         "team_score_home": 0.0, "team_score_away": 1.0,
         "event_result": "away", "game_order": 3},
    ])
