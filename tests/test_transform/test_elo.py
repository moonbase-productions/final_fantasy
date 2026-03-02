"""tests/test_transform/test_elo.py"""
from __future__ import annotations

import pytest
import pandas as pd

from pipeline.transform.elo import compute_elo_stats


def test_returns_two_dataframes(minimal_events):
    summary, history = compute_elo_stats(minimal_events)
    assert isinstance(summary, pd.DataFrame)
    assert isinstance(history, pd.DataFrame)


def test_summary_has_required_columns(minimal_events):
    summary, _ = compute_elo_stats(minimal_events)
    required = {
        "uid", "league_id", "league_season",
        "start_of_season_elo", "end_of_season_elo",
        "last_elo_delta", "season_elo_delta",
    }
    assert required.issubset(set(summary.columns))


def test_history_has_required_columns(minimal_events):
    _, history = compute_elo_stats(minimal_events)
    required = {"uid", "league_id", "league_season",
                "event_id", "event_date",
                "actual_result", "expected_win_prob", "current_elo"}
    assert required.issubset(set(history.columns))


def test_winner_elo_increases(minimal_events):
    """After a win the team's Elo should be higher than its starting value."""
    summary, _ = compute_elo_stats(minimal_events)
    # Team 100-1 has 2 wins and 1 loss, net positive expected
    t1 = summary[summary["uid"] == "100-1"].iloc[0]
    assert t1["season_elo_delta"] > 0


def test_both_teams_in_summary(minimal_events):
    summary, _ = compute_elo_stats(minimal_events)
    uids = set(summary["uid"].tolist())
    assert "100-1" in uids
    assert "100-2" in uids


def test_start_of_season_elo_uses_pre_game_baseline(minimal_events):
    """Season start Elo should be the pre-first-game rating, not post-game Elo."""
    summary, _ = compute_elo_stats(minimal_events)
    starts = summary.set_index("uid")["start_of_season_elo"].to_dict()
    assert starts["100-1"] == 1500.0
    assert starts["100-2"] == 1500.0


def test_elo_sum_conserved(minimal_events):
    """Total Elo across teams must be conserved (zero-sum) to within rounding."""
    summary, _ = compute_elo_stats(minimal_events)
    latest = (
        summary.sort_values("league_season", ascending=False)
        .groupby("uid").first()
        .reset_index()
    )
    total_delta = latest["season_elo_delta"].sum()
    assert abs(total_delta) < 1.0  # rounding tolerance


def test_empty_input():
    empty = pd.DataFrame(columns=["event_id", "league_id", "league_season",
                                   "event_date", "league_sport", "uid_home",
                                   "uid_away", "team_score_home",
                                   "team_score_away", "event_result"])
    summary, history = compute_elo_stats(empty)
    assert summary.empty
    assert history.empty
