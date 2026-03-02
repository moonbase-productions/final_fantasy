"""tests/test_transform/test_stats.py"""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.transform.stats import compute_basic_stats


def test_wins_losses_draws(minimal_events):
    result = compute_basic_stats(minimal_events)
    t1 = result[result["uid"] == "100-1"].iloc[0]
    t2 = result[result["uid"] == "100-2"].iloc[0]

    # Team 1: 2 wins (events 1, 5), 1 loss (event 3), 2 draws (events 2, 4)
    assert t1["wins"]  == 2
    assert t1["losses"] == 1
    assert t1["draws"] == 2

    # Team 2: 1 win (event 3), 2 losses (events 1, 5), 2 draws
    assert t2["wins"]  == 1
    assert t2["losses"] == 2
    assert t2["draws"] == 2


def test_win_percentage(minimal_events):
    result = compute_basic_stats(minimal_events)
    t1 = result[result["uid"] == "100-1"].iloc[0]
    # win_pct = (2 + 0.5*2) / 5 = 3/5 = 0.6
    assert abs(t1["win_percentage"] - 0.60) < 0.01


def test_games_played(minimal_events):
    result = compute_basic_stats(minimal_events)
    for uid in ("100-1", "100-2"):
        row = result[result["uid"] == uid].iloc[0]
        assert row["games_played"] == 5


def test_empty_input():
    empty = pd.DataFrame(columns=["event_id", "league_id", "league_season",
                                   "event_date", "league_sport", "uid_home",
                                   "uid_away", "team_score_home",
                                   "team_score_away", "event_result"])
    result = compute_basic_stats(empty)
    assert result.empty


def test_binary_events_stats(binary_events):
    result = compute_basic_stats(binary_events)
    # uid 200-10: 1 win (b1), 1 loss (b3) → 2 games
    t10 = result[result["uid"] == "200-10"].iloc[0]
    assert t10["games_played"] == 2
    assert t10["wins"] == 1
    assert t10["losses"] == 1
