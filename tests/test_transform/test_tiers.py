"""tests/test_transform/test_tiers.py"""
from __future__ import annotations

import pandas as pd
import pytest

from pipeline.transform.tiers import compute_tiers


def _make_elo_summary(elos: list[float]) -> pd.DataFrame:
    """Create a minimal elo_summary DataFrame with the given Elo values."""
    return pd.DataFrame({
        "uid":                [f"100-{i}" for i in range(len(elos))],
        "league_id":          ["100"] * len(elos),
        "league_season":      ["2024"] * len(elos),
        "end_of_season_elo":  elos,
    })


def test_returns_list_of_dicts():
    df = _make_elo_summary([1400.0, 1500.0, 1600.0])
    result = compute_tiers(df)
    assert isinstance(result, list)
    assert all(isinstance(r, dict) for r in result)


def test_required_keys():
    df = _make_elo_summary([1400.0, 1500.0, 1600.0])
    result = compute_tiers(df)
    for row in result:
        assert "uid" in row
        assert "tier" in row
        assert "league_id" in row


def test_highest_elo_gets_high_tier():
    """With 1000 teams, top 0.5% should be MOL."""
    import random
    elos = [random.uniform(1200, 1800) for _ in range(200)]
    elos.append(9999.0)  # guaranteed top
    df = _make_elo_summary(elos)
    result = compute_tiers(df)
    top_row = next(r for r in result if r["uid"] == f"100-{len(elos)-1}")
    assert top_row["tier"] == "MOL"


def test_lowest_elo_gets_die():
    """Lowest Elo should get DIE tier."""
    elos = [float(i * 10) for i in range(1, 201)]
    elos[0] = 1.0  # guaranteed lowest
    df = _make_elo_summary(elos)
    result = compute_tiers(df)
    bottom = next(r for r in result if r["uid"] == "100-0")
    assert bottom["tier"] == "DIE"


def test_empty_input():
    result = compute_tiers(pd.DataFrame())
    assert result == []
