"""tests/test_integration/test_smoke.py

Full transform chain smoke test. Does not require a database connection.
Uses synthetic events to validate that all transform functions compose
correctly and produce output with the right shapes.
"""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.transform.stats import compute_basic_stats
from pipeline.transform.elo   import compute_elo_stats
from pipeline.transform.tiers import compute_tiers
from pipeline.transform.luck  import compute_luck


def _generate_round_robin(n_teams: int = 8, n_seasons: int = 2) -> pd.DataFrame:
    """Generate a synthetic round-robin tournament for n_teams over n_seasons."""
    import random
    rows = []
    eid = 0
    for season_i in range(n_seasons):
        season = f"202{season_i}"
        for home_i in range(n_teams):
            for away_i in range(n_teams):
                if home_i == away_i:
                    continue
                h_score = random.randint(0, 4)
                a_score = random.randint(0, 4)
                result = "draw" if h_score == a_score else ("home" if h_score > a_score else "away")
                rows.append({
                    "event_id":       f"e{eid}",
                    "league_id":      "999",
                    "league_season":  season,
                    "event_date":     f"202{season_i}-03-{(eid % 28) + 1:02d}",
                    "league_sport":   "Soccer",
                    "uid_home":       f"999-{home_i}",
                    "uid_away":       f"999-{away_i}",
                    "team_score_home": float(h_score),
                    "team_score_away": float(a_score),
                    "event_result":   result,
                    "game_order":     eid,
                })
                eid += 1
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def synthetic_events():
    return _generate_round_robin(n_teams=8, n_seasons=2)


def test_full_chain_runs(synthetic_events):
    basic_df            = compute_basic_stats(synthetic_events)
    elo_df, history_df  = compute_elo_stats(synthetic_events)
    tiers_data          = compute_tiers(elo_df)
    luck_df             = compute_luck(history_df)

    assert not basic_df.empty
    assert not elo_df.empty
    assert not history_df.empty
    assert len(tiers_data) > 0
    assert not luck_df.empty


def test_all_teams_in_output(synthetic_events):
    _, elo_df = compute_elo_stats(synthetic_events)
    summary = elo_df
    uids_in = set(
        synthetic_events["uid_home"].tolist()
        + synthetic_events["uid_away"].tolist()
    )
    uids_out = set(summary["uid"].tolist())
    assert uids_in == uids_out


def test_luck_display_all_valid(synthetic_events):
    _, history_df = compute_elo_stats(synthetic_events)
    luck_df = compute_luck(history_df)
    assert (luck_df["luck_display"] >= 0).all()
    assert (luck_df["luck_display"] <= 100).all()


def test_tier_for_every_team(synthetic_events):
    elo_df, _ = compute_elo_stats(synthetic_events)
    tiers = compute_tiers(elo_df)
    uids_in = set(synthetic_events["uid_home"].unique())
    uids_tiered = {t["uid"] for t in tiers}
    assert uids_in == uids_tiered
